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

func TestSessionGracefulCloseDecodesBufferedTrailers(t *testing.T) {
	transport := &dispatchTransport{streams: make(chan ReceiveStream, 2)}
	feedback := &fakeStream{interrupt: make(chan struct{})}
	local := &LocalStreams{Control: &fakeStream{interrupt: make(chan struct{})}, Encoder: &fakeStream{interrupt: make(chan struct{})}, Decoder: feedback}
	settings := []h3.Setting{{ID: h3.QPACKMaxTableCapacity, Value: 128}, {ID: h3.QPACKBlockedStreams, Value: 4}}
	parent, stop := context.WithCancelCause(context.Background())
	defer stop(context.Canceled)
	session, err := StartSession(parent, transport, local, SessionConfig{PeerIsServer: true, MaxRequests: 1, LocalSettings: settings, Decoder: decoderLimits(), Encoder: encoderLimits(), MaxActiveStreams: 4, MaxSettingsBytes: 1024, MaxSettingsEntries: 8, MaxEncoderInstruction: 1024, Control: func(context.Context, h3.ControlEvent) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Stop(context.Canceled)
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
	wire := readerHeaders(t, responseFields("200")...)
	wire = append(wire, readerFrame(t, h3.Data, []byte("accepted"))...)
	// The trailer section references the dynamic entry, so decoding it owes a
	// section acknowledgment that can no longer be delivered.
	wire = append(wire, readerFrame(t, h3.Headers, []byte{2, 0, 0x80})...)
	sendCtx, sendCancel := context.WithCancelCause(context.Background())
	defer sendCancel(context.Canceled)
	stream := &exchangeStream{serveStream: &serveStream{source: bytes.NewReader(wire)}, sendCtx: sendCtx, sendCancel: sendCancel}
	request, _ := http.NewRequest("GET", "https://example.com/", nil)
	response, err := session.ExchangeRequest(request, stream, ClientExchangeConfig{MaxEncodedHeaders: 4096, MaxInformational: 4})
	if err != nil {
		t.Fatal(err)
	}
	session.receiveGoaway(PeerGoaway{ID: 4, RequestIDs: true})
	peerClose := &quic.PeerCloseError{Application: true, Code: 0x100, Reason: "drained"}
	stop(peerClose)
	select {
	case <-session.Done():
	case <-time.After(time.Second):
		t.Fatal("session did not stop")
	}
	feedback.fail = peerClose
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != "accepted" || response.Trailer.Get("X-Drained") != "complete" {
		t.Fatalf("buffered response lost after graceful close: %q %v %v", body, response.Trailer, err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if transport.aborts != 0 {
		t.Fatal("graceful drain aborted the connection", transport.code)
	}
}

func TestFieldDecoderDetach(t *testing.T) {
	writer := &failedEncoderWriter{}
	d, _ := NewFieldDecoder(decoderLimits(), writer)
	decoderUpdate(t, d, qpack.EncoderInstruction{Type: qpack.SetDynamicCapacity, Index: 128})
	decoderUpdate(t, d, qpack.EncoderInstruction{Type: qpack.InsertLiteralName, Name: "x", Value: "value"})
	blocked, err := d.Submit(0, []byte{3, 0, 0x80})
	if err != nil {
		t.Fatal(err)
	}
	d.Flushed()
	writes := writer.writes
	cause := errors.New("peer drained")
	d.Detach(cause)
	if readySection(t, blocked).Err != cause {
		t.Fatal("detach did not release blocked section")
	}
	ready, err := d.Submit(4, []byte{2, 0, 0x80})
	if err != nil {
		t.Fatal(err)
	}
	if result := readySection(t, ready); result.Err != nil || len(result.Fields) != 1 || result.Fields[0] != fs("x", "value") {
		t.Fatal(result)
	}
	if _, err := d.Submit(8, []byte{3, 0, 0x80}); err != cause {
		t.Fatal("unresolvable section accepted", err)
	}
	if d.Cancel(4) != cause || d.EncoderInstruction(nil) != cause {
		t.Fatal("lost detach cause")
	}
	d.Flushed()
	if writer.writes != writes {
		t.Fatal("feedback written after detach")
	}
	terminal := errors.New("stopped")
	d.Close(terminal)
	if _, err := d.Submit(12, []byte{0, 0}); err != terminal {
		t.Fatal(err)
	}
}
