package http3conn

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
)

type drainFixture struct {
	session   *Session
	transport *dispatchTransport
	feedback  *fakeStream
	stop      context.CancelCauseFunc
}

// The peer has sent SETTINGS and one dynamic table entry ("x-drained").
func drainSession(t *testing.T) *drainFixture {
	t.Helper()
	transport := &dispatchTransport{streams: make(chan ReceiveStream, 2)}
	feedback := &fakeStream{interrupt: make(chan struct{})}
	local := &LocalStreams{Control: &fakeStream{interrupt: make(chan struct{})}, Encoder: &fakeStream{interrupt: make(chan struct{})}, Decoder: feedback}
	settings := []h3.Setting{{ID: h3.QPACKMaxTableCapacity, Value: 128}, {ID: h3.QPACKBlockedStreams, Value: 4}}
	parent, stop := context.WithCancelCause(context.Background())
	t.Cleanup(func() { stop(context.Canceled) })
	session, err := StartSession(parent, transport, local, SessionConfig{PeerIsServer: true, MaxRequests: 1, LocalSettings: settings, Decoder: decoderLimits(), Encoder: encoderLimits(), MaxActiveStreams: 4, MaxSettingsBytes: 1024, MaxSettingsEntries: 8, MaxEncoderInstruction: 1024, Control: func(context.Context, h3.ControlEvent) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Stop(context.Canceled) })
	updates := []byte{2}
	for _, update := range []qpack.EncoderInstruction{{Type: qpack.SetDynamicCapacity, Index: 128}, {Type: qpack.InsertLiteralName, Name: "x-drained", Value: "complete"}} {
		if updates, err = qpack.AppendEncoderInstruction(updates, update); err != nil {
			t.Fatal(err)
		}
	}
	encoder := heldSessionStream(7, updates)
	control, err := h3.AppendSettings([]byte{0}, nil)
	if err != nil {
		t.Fatal(err)
	}
	critical := heldSessionStream(3, control)
	transport.streams <- encoder
	transport.streams <- critical
	for _, stream := range []*heldSessionReceive{encoder, critical} {
		select {
		case <-stream.held:
		case <-time.After(time.Second):
			t.Fatal("critical stream not consumed")
		}
	}
	return &drainFixture{session: session, transport: transport, feedback: feedback, stop: stop}
}

func (f *drainFixture) joined(t *testing.T) {
	t.Helper()
	select {
	case <-f.session.Done():
	case <-time.After(time.Second):
		t.Fatal("session did not stop")
	}
	f.transport.mu.Lock()
	defer f.transport.mu.Unlock()
	if f.transport.aborts != 0 {
		t.Fatal("graceful drain aborted the connection", f.transport.code)
	}
}

// The application reads buffered dynamic trailers before the session has
// detached the decoder: either before the session has even observed the close,
// or while its shutdown is held just short of Detach.
func TestSessionGracefulCloseDecodesTrailersBeforeDetach(t *testing.T) {
	for _, observed := range []bool{false, true} {
		name := "close-unobserved"
		if observed {
			name = "shutdown-gated"
		}
		t.Run(name, func(t *testing.T) {
			f := drainSession(t)
			wire := readerHeaders(t, responseFields("200")...)
			wire = append(wire, readerFrame(t, h3.Data, []byte("accepted"))...)
			wire = append(wire, readerFrame(t, h3.Headers, []byte{2, 0, 0x80})...)
			sendCtx, sendCancel := context.WithCancelCause(context.Background())
			defer sendCancel(context.Canceled)
			stream := &exchangeStream{serveStream: &serveStream{source: bytes.NewReader(wire)}, sendCtx: sendCtx, sendCancel: sendCancel}
			request, _ := http.NewRequest("GET", "https://example.com/", nil)
			response, err := f.session.ExchangeRequest(request, stream, ClientExchangeConfig{MaxEncodedHeaders: 4096, MaxInformational: 4})
			if err != nil {
				t.Fatal(err)
			}
			f.session.receiveGoaway(PeerGoaway{ID: 4, RequestIDs: true})
			peerClose := &quic.PeerCloseError{Application: true, Code: 0x100, Reason: "drained"}
			// Shutdown joins control writes before it decides the decoder's fate.
			f.session.controlMu.Lock()
			gated := true
			defer func() {
				if gated {
					f.session.controlMu.Unlock()
				}
			}()
			f.feedback.fail = peerClose
			if observed {
				f.stop(peerClose)
				<-f.feedback.interrupt
			}
			body, err := io.ReadAll(response.Body)
			if err != nil || string(body) != "accepted" || response.Trailer.Get("X-Drained") != "complete" {
				t.Fatalf("buffered response lost before detach: %q %v %v", body, response.Trailer, err)
			}
			select {
			case <-f.session.Done():
				t.Fatal("shutdown was not gated")
			default:
			}
			gated = false
			f.session.controlMu.Unlock()
			f.joined(t)
			if _, err := f.session.WaitEncoder(context.Background()); !errors.Is(err, peerClose) {
				t.Fatal("session cause", err)
			}
			if err := response.Body.Close(); err != nil {
				t.Fatal(err)
			}
			// Other buffered responses still decode against the kept table.
			ready, err := f.session.Decoder().Submit(4, []byte{2, 0, 0x80})
			if err != nil {
				t.Fatal("table discarded after detach:", err)
			}
			if result := readySection(t, ready); result.Err != nil || len(result.Fields) != 1 || result.Fields[0] != fs("x-drained", "complete") {
				t.Fatal(result)
			}
		})
	}
}

// The peer closes gracefully without first sending STOP_SENDING, so only the
// connection close ends the blocked upload of an already answered request.
func TestSessionGracefulCloseStopsUploadKeepsResponse(t *testing.T) {
	f := drainSession(t)
	wire := readerHeaders(t, responseFields("200")...)
	wire = append(wire, readerFrame(t, h3.Data, []byte("accepted"))...)
	sendCtx, sendCancel := context.WithCancelCause(context.Background())
	defer sendCancel(context.Canceled)
	stream := &exchangeStream{serveStream: &serveStream{source: bytes.NewReader(wire)}, sendCtx: sendCtx, sendCancel: sendCancel}
	pipe, producer := io.Pipe()
	defer producer.Close()
	entered, closed := make(chan struct{}), make(chan struct{})
	upload := &uploadBody{read: func(p []byte) (int, error) { close(entered); return pipe.Read(p) }, close: func() error { defer close(closed); return pipe.Close() }}
	request, _ := http.NewRequest("POST", "https://example.com/", upload)
	response, err := f.session.ExchangeRequest(request, stream, ClientExchangeConfig{MaxEncodedHeaders: 4096, MaxInformational: 4})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("upload did not start")
	}
	f.session.receiveGoaway(PeerGoaway{ID: 4, RequestIDs: true})
	peerClose := &quic.PeerCloseError{Application: true, Code: 0x100, Reason: "drained"}
	f.stop(peerClose)
	sendCancel(peerClose)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("connection close did not stop the blocked upload")
	}
	f.joined(t)
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != "accepted" {
		t.Fatalf("accepted response canceled by graceful upload stop: %q %v", body, err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal("close after complete response:", err)
	}
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if len(stream.readCodes) != 0 {
		t.Fatal("response reads canceled", stream.readCodes)
	}
}

func TestFieldDecoderUndeliverableFeedbackDetaches(t *testing.T) {
	cause := errors.New("peer drained")
	for _, operation := range []string{"submit", "instruction", "cancel"} {
		t.Run(operation, func(t *testing.T) {
			writer := &failedEncoderWriter{}
			d, _ := NewFieldDecoder(decoderLimits(), writer)
			reports := 0
			d.feedbackFailed = func(error) error { reports++; return cause }
			decoderUpdate(t, d, qpack.EncoderInstruction{Type: qpack.SetDynamicCapacity, Index: 128})
			unblocked, err := d.Submit(0, []byte{2, 0, 0x80})
			if err != nil {
				t.Fatal(err)
			}
			stuck, err := d.Submit(4, []byte{3, 0, 0x80})
			if err != nil {
				t.Fatal(err)
			}
			insert := qpack.EncoderInstruction{Type: qpack.InsertLiteralName, Name: "x", Value: "value"}
			if operation != "instruction" {
				if err := decoderUpdate(t, d, insert); err != nil {
					t.Fatal(err)
				}
			}
			writer.fail = true
			switch operation {
			case "submit":
				ready, err := d.Submit(8, []byte{2, 0, 0x80})
				if err != nil {
					t.Fatal(err)
				}
				if result := readySection(t, ready); result.Err != nil || len(result.Fields) != 1 {
					t.Fatal(result)
				}
			case "instruction":
				// The insert is applied even though its acknowledgment is lost.
				if err := decoderUpdate(t, d, insert); err != cause {
					t.Fatal(err)
				}
			case "cancel":
				if err := d.Cancel(8); err != cause {
					t.Fatal(err)
				}
			}
			if result := readySection(t, unblocked); result.Err != nil || len(result.Fields) != 1 || result.Fields[0] != fs("x", "value") {
				t.Fatal("section completed by the insert", result)
			}
			if readySection(t, stuck).Err != cause {
				t.Fatal("unresolvable section not released")
			}
			if reports != 1 {
				t.Fatal("owner reports", reports)
			}
			writes := writer.writes
			ready, err := d.Submit(12, []byte{2, 0, 0x80})
			if err != nil {
				t.Fatal("table discarded:", err)
			}
			if result := readySection(t, ready); result.Err != nil || len(result.Fields) != 1 || result.Fields[0] != fs("x", "value") {
				t.Fatal(result)
			}
			if writer.writes != writes {
				t.Fatal("feedback written after detach")
			}
		})
	}
}
