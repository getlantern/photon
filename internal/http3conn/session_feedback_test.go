package http3conn

import (
	"context"
	"errors"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
)

func TestSessionRequestFeedbackFailureStopsSession(t *testing.T) {
	stopped := &quic.StreamError{Remote: true, ErrorCode: 0x104}
	for _, tc := range []struct {
		name   string
		cause  error
		submit bool
		aborts bool
	}{
		{"submit", stopped, true, true},
		{"cancel", stopped, false, true},
		{"early-rejection", quic.Err0RTTRejected, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transport := &dispatchTransport{streams: make(chan ReceiveStream, 1)}
			feedback := &fakeStream{interrupt: make(chan struct{})}
			local := &LocalStreams{Control: &fakeStream{interrupt: make(chan struct{})}, Encoder: &fakeStream{interrupt: make(chan struct{})}, Decoder: feedback}
			settings := []h3.Setting{{ID: h3.QPACKMaxTableCapacity, Value: 128}, {ID: h3.QPACKBlockedStreams, Value: 4}}
			session, err := StartSession(context.Background(), transport, local, SessionConfig{PeerIsServer: true, LocalSettings: settings, Decoder: decoderLimits(), Encoder: encoderLimits(), MaxActiveStreams: 4, MaxSettingsBytes: 1024, MaxSettingsEntries: 8, MaxEncoderInstruction: 1024, Control: func(context.Context, h3.ControlEvent) error { return nil }})
			if err != nil {
				t.Fatal(err)
			}
			defer session.Stop(context.Canceled)
			updates := []byte{2}
			for _, update := range []qpack.EncoderInstruction{{Type: qpack.SetDynamicCapacity, Index: 128}, {Type: qpack.InsertLiteralName, Name: "x", Value: "value"}} {
				if updates, err = qpack.AppendEncoderInstruction(updates, update); err != nil {
					t.Fatal(err)
				}
			}
			encoder := heldSessionStream(7, updates)
			transport.streams <- encoder
			select {
			case <-encoder.held:
			case <-time.After(time.Second):
				t.Fatal("encoder stream not consumed")
			}
			// The peer stops the decoder stream; only feedback for a request
			// notices. The request-side call has queued it and succeeds.
			session.Decoder().Flushed()
			feedback.setFail(tc.cause)
			if tc.submit {
				_, err = session.Decoder().Submit(0, []byte{2, 0, 0x80})
			} else {
				err = session.Decoder().Cancel(0)
			}
			if err != nil {
				t.Fatal("request-side call", err)
			}
			select {
			case <-session.Done():
			case <-time.After(time.Second):
				t.Fatal("session survived an unusable decoder")
			}
			if _, err := session.WaitEncoder(context.Background()); !errors.Is(err, tc.cause) {
				t.Fatal("session cause", err)
			}
			transport.mu.Lock()
			defer transport.mu.Unlock()
			if tc.aborts && (transport.aborts != 1 || transport.code != InternalError) || !tc.aborts && transport.aborts != 0 {
				t.Fatal("connection scope", transport.aborts, transport.code)
			}
		})
	}
}

// A peer that leaves the decoder stream without credit until the feedback
// backlog overflows gets a connection error, and the session still stops: the
// insertion that overflowed it had sections to complete at that moment.
func TestSessionFeedbackBacklogOverflowStopsSession(t *testing.T) {
	transport := &dispatchTransport{streams: make(chan ReceiveStream, 1)}
	feedback := &fakeStream{block: true, entered: make(chan struct{}), interrupt: make(chan struct{})}
	local := &LocalStreams{Control: &fakeStream{interrupt: make(chan struct{})}, Encoder: &fakeStream{interrupt: make(chan struct{})}, Decoder: feedback}
	settings := []h3.Setting{{ID: h3.QPACKMaxTableCapacity, Value: 128}, {ID: h3.QPACKBlockedStreams, Value: 4}}
	session, err := StartSession(context.Background(), transport, local, SessionConfig{PeerIsServer: true, LocalSettings: settings, Decoder: decoderLimits(), Encoder: encoderLimits(), MaxActiveStreams: 4, MaxSettingsBytes: 1024, MaxSettingsEntries: 8, MaxEncoderInstruction: 1024, Control: func(context.Context, h3.ControlEvent) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	updates := []byte{2}
	for _, update := range []qpack.EncoderInstruction{{Type: qpack.SetDynamicCapacity, Index: 128}, insertX} {
		if updates, err = qpack.AppendEncoderInstruction(updates, update); err != nil {
			t.Fatal(err)
		}
	}
	transport.streams <- heldSessionStream(7, updates)
	// The first insertion's feedback is stuck on the decoder stream.
	select {
	case <-feedback.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("no feedback reached the decoder stream")
	}
	decoder := session.Decoder()
	first, _ := decoder.Submit(0, []byte{3, 0, 0x80})
	second, _ := decoder.Submit(4, []byte{3, 0, 0x80})
	decoder.mu.Lock()
	decoder.maxQueued = len(decoder.queue)
	decoder.mu.Unlock()
	wire, _ := qpack.AppendEncoderInstruction(nil, insertX)
	var protocol *h3.Error
	returnsPromptly(t, "the overflowing insertion", func() { err = decoder.EncoderInstruction(wire) })
	if !errors.As(err, &protocol) || protocol.Code != h3.ExcessiveLoad {
		t.Fatal(err)
	}
	for _, result := range []<-chan DecodedSection{first, second} {
		if decoded := readySection(t, result); decoded.Err != nil || len(decoded.Fields) != 1 {
			t.Fatal("section the insertion completed:", decoded)
		}
	}
	returnsPromptly(t, "Session.Stop", func() { session.Stop(context.Canceled) })
	transport.check(t, h3.ExcessiveLoad)
}
