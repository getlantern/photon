package http3conn

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/wire"
)

func TestSessionInterruptsBlockedEncoderBeforeJoin(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		name := "parent cancellation"
		if duplicate {
			name = "duplicate critical stream"
		}
		t.Run(name, func(t *testing.T) {
			transport := &dispatchTransport{streams: make(chan ReceiveStream, 2)}
			encoder := &fakeStream{block: true, entered: make(chan struct{}), interrupt: make(chan struct{})}
			local := &LocalStreams{Control: &fakeStream{interrupt: make(chan struct{})}, Encoder: encoder, Decoder: &fakeStream{interrupt: make(chan struct{})}}
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(context.Canceled)
			session, err := StartSession(ctx, transport, local, SessionConfig{PeerIsServer: true, Decoder: decoderLimits(), Encoder: encoderLimits(), MaxActiveStreams: 4, MaxSettingsBytes: 1024, MaxSettingsEntries: 8, MaxEncoderInstruction: 1024, Control: func(context.Context, h3.ControlEvent) error { return nil }})
			if err != nil {
				t.Fatal(err)
			}
			wire, err := h3.AppendSettings([]byte{0}, []h3.Setting{{ID: h3.QPACKMaxTableCapacity, Value: 128}})
			if err != nil {
				t.Fatal(err)
			}
			transport.streams <- &fakeReceive{Reader: bytes.NewReader(wire), id: quic.StreamID(3)}
			select {
			case <-encoder.entered:
			case <-time.After(time.Second):
				t.Fatal("encoder initialization did not block")
			}
			cause := errors.New("endpoint stopped")
			if duplicate {
				transport.streams <- receive(7, 0)
			} else {
				cancel(cause)
			}
			select {
			case <-session.Done():
			case <-time.After(time.Second):
				cancel(cause)
				t.Fatal("session stranded in encoder write")
			}
			_, err = session.WaitEncoder(context.Background())
			if duplicate {
				var protocol *h3.Error
				if !errors.As(err, &protocol) || protocol.Code != h3.StreamCreationError {
					t.Fatal(err)
				}
				transport.check(t, h3.StreamCreationError)
			} else if err != cause {
				t.Fatal(err)
			}
			// Stop is joinable and idempotent even after a protocol failure.
			session.Stop(context.Canceled)
			session.Stop(context.Canceled)
			if _, err := session.Decoder().Submit(0, []byte{0, 0}); err == nil {
				t.Fatal("decoder survived stopped session")
			}
		})
	}
}

// Remains open after its fixture bytes; reaching held proves the previous
// instruction callback returned. A deadline releases the read during shutdown.
type heldSessionReceive struct {
	*fakeReceive
	held, interrupted       chan struct{}
	holdOnce, interruptOnce sync.Once
}

func (s *heldSessionReceive) Read(p []byte) (int, error) {
	n, err := s.Reader.Read(p)
	if err != io.EOF {
		return n, err
	}
	s.holdOnce.Do(func() { close(s.held) })
	<-s.interrupted
	return 0, context.Canceled
}
func (s *heldSessionReceive) SetReadDeadline(at time.Time) error {
	if !at.IsZero() {
		s.interruptOnce.Do(func() { close(s.interrupted) })
	}
	return nil
}
func heldSessionStream(id quic.StreamID, wire []byte) *heldSessionReceive {
	return &heldSessionReceive{fakeReceive: &fakeReceive{Reader: bytes.NewReader(wire), id: id}, held: make(chan struct{}), interrupted: make(chan struct{})}
}
func TestSessionFeedbackBeforeSettings(t *testing.T) {
	for _, instruction := range []byte{0x40, 0x80, 1} {
		name := "cancel"
		if instruction == 0x80 {
			name = "ACK"
		}
		if instruction == 1 {
			name = "increment"
		}
		t.Run(name, func(t *testing.T) {
			transport := &dispatchTransport{streams: make(chan ReceiveStream, 2)}
			local := &LocalStreams{Control: &fakeStream{interrupt: make(chan struct{})}, Encoder: &fakeStream{interrupt: make(chan struct{})}, Decoder: &fakeStream{interrupt: make(chan struct{})}}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			session, err := StartSession(ctx, transport, local, SessionConfig{PeerIsServer: true, Decoder: decoderLimits(), Encoder: encoderLimits(), MaxActiveStreams: 4, MaxSettingsBytes: 1024, MaxSettingsEntries: 8, MaxEncoderInstruction: 1024, Control: func(context.Context, h3.ControlEvent) error { return nil }})
			if err != nil {
				t.Fatal(err)
			}
			defer session.Stop(context.Canceled)
			feedback := heldSessionStream(3, []byte{3, instruction})
			transport.streams <- feedback
			if instruction != 0x40 {
				select {
				case <-session.Done():
				case <-ctx.Done():
					t.Fatal("invalid feedback did not terminate")
				}
				transport.check(t, QPACKDecoderStreamError)
				return
			}
			select {
			case <-feedback.held:
			case <-session.Done():
				t.Fatal("cancellation rejected before SETTINGS")
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			waiting, cancelWait := context.WithCancel(ctx)
			cancelWait()
			if _, err := session.WaitEncoder(waiting); err != context.Canceled {
				t.Fatal(err)
			}
			// Canceling a waiter must not prevent SETTINGS and publication afterward.
			control := heldSessionStream(7, []byte{0, 4, 0})
			transport.streams <- control
			encoder, err := session.WaitEncoder(ctx)
			if err != nil || encoder == nil {
				t.Fatal(encoder, err)
			}
			if _, err := encoder.Encode(4, responseFields("200")); err != nil {
				t.Fatal(err)
			}
			transport.mu.Lock()
			aborts := transport.aborts
			transport.mu.Unlock()
			if aborts != 0 {
				t.Fatal("aborted legal cancellation")
			}
		})
	}
}

func TestSessionGoawayPublication(t *testing.T) {
	for _, peerIsServer := range []bool{false, true} {
		name := "push IDs"
		if peerIsServer {
			name = "request IDs"
		}
		t.Run(name, func(t *testing.T) {
			transport := &dispatchTransport{streams: make(chan ReceiveStream, 1)}
			local := &LocalStreams{Control: &fakeStream{interrupt: make(chan struct{})}, Encoder: &fakeStream{interrupt: make(chan struct{})}, Decoder: &fakeStream{interrupt: make(chan struct{})}}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			events := make(chan h3.ControlEvent)
			release := make(chan struct{})
			session, err := StartSession(ctx, transport, local, SessionConfig{PeerIsServer: peerIsServer, MaxRequests: 1, Decoder: decoderLimits(), Encoder: encoderLimits(), MaxActiveStreams: 4, MaxSettingsBytes: 1024, MaxSettingsEntries: 8, MaxEncoderInstruction: 1024, Control: func(ctx context.Context, event h3.ControlEvent) error {
				select {
				case events <- event:
				case <-ctx.Done():
					return ctx.Err()
				}
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}})
			if err != nil {
				t.Fatal(err)
			}
			defer session.Stop(context.Canceled)
			if _, ok := session.Goaway(); ok {
				t.Fatal("draining before GOAWAY")
			}
			var request *RequestLease
			if peerIsServer {
				request, err = session.BeginRequest(8)
				if err != nil {
					t.Fatal(err)
				}
				defer request.Finish()
			}
			maximum := wire.MaxVarint
			if peerIsServer {
				maximum -= 3
			}
			limits := []uint64{maximum, 8, 8, 0}
			payload := []byte{0, 4, 0}
			for _, id := range limits {
				b, _ := wire.AppendVarint(nil, id)
				payload = append(payload, readerFrame(t, h3.Goaway, b)...)
			}
			id := quic.StreamID(2)
			if peerIsServer {
				id = 3
			}
			transport.streams <- heldSessionStream(id, payload)
			for _, limit := range limits {
				select {
				case event := <-events:
					if event.Type != h3.Goaway || event.ID != limit {
						t.Fatal(event)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if request != nil {
					cause := context.Cause(request.Context())
					if limit <= 8 && cause != ErrRequestRejected {
						t.Fatal("GOAWAY not applied before callback", cause)
					}
					if limit > 8 && cause != nil {
						t.Fatal("request below limit canceled", cause)
					}
				}
				state, ok := session.Goaway()
				if !ok || state.ID != limit || state.RequestIDs != peerIsServer {
					t.Fatal(state, ok)
				}
				if !state.Rejects(limit) || !state.Rejects(maximum) {
					t.Fatal("rejected work misclassified")
				}
				if limit > 0 && state.Rejects(0) {
					t.Fatal("earlier work marked rejected")
				}
				// Returned values are snapshots; mutation cannot change session state.
				state.ID = 123
				current, _ := session.Goaway()
				if current.ID != limit {
					t.Fatal("mutable state escaped")
				}
				select {
				case release <- struct{}{}:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
		})
	}
}

func sendingSession(t *testing.T, peerIsServer bool, control *fakeStream) (*Session, *dispatchTransport) {
	t.Helper()
	transport := &dispatchTransport{streams: make(chan ReceiveStream)}
	local := &LocalStreams{Control: control, Encoder: &fakeStream{interrupt: make(chan struct{})}, Decoder: &fakeStream{interrupt: make(chan struct{})}}
	session, err := StartSession(context.Background(), transport, local, SessionConfig{PeerIsServer: peerIsServer, Decoder: decoderLimits(), Encoder: encoderLimits(), MaxActiveStreams: 4, MaxSettingsBytes: 1024, MaxSettingsEntries: 8, MaxEncoderInstruction: 1024, Control: func(context.Context, h3.ControlEvent) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Stop(context.Canceled) })
	return session, transport
}
func TestSessionSendGoaway(t *testing.T) {
	for _, server := range []bool{false, true} {
		name := "client"
		if server {
			name = "server"
		}
		t.Run(name, func(t *testing.T) {
			control := &fakeStream{interrupt: make(chan struct{})}
			session, _ := sendingSession(t, !server, control)
			maximum := wire.MaxVarint
			if server {
				maximum -= 3
			}
			if err := session.SendGoaway(wire.MaxVarint + 1); err != ErrGoawayID {
				t.Fatal(err)
			}
			if server {
				if err := session.SendGoaway(1); err != ErrGoawayID {
					t.Fatal(err)
				}
			}
			limits := []uint64{maximum, 8, 8, 0}
			for _, id := range limits {
				if err := session.SendGoaway(id); err != nil {
					t.Fatal(err)
				}
			}
			before := control.Len()
			if err := session.SendGoaway(4); err != ErrGoawayID || control.Len() != before {
				t.Fatal(err)
			}
			// Validate actual wire bytes through the separate role-aware control parser.
			data := append([]byte{4, 0}, control.Bytes()...)
			reader, err := h3.NewControlReader(bytes.NewReader(data), server, 1024, 8)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = reader.Next(); err != nil {
				t.Fatal(err)
			}
			for _, id := range limits {
				event, err := reader.Next()
				if err != nil || event.Type != h3.Goaway || event.ID != id {
					t.Fatal(event, err)
				}
			}
		})
	}
}
func TestSessionSendGoawayFailure(t *testing.T) {
	control := &fakeStream{short: true, interrupt: make(chan struct{})}
	session, transport := sendingSession(t, false, control)
	if err := session.SendGoaway(8); err != io.ErrShortWrite {
		t.Fatal(err)
	}
	if err := session.SendGoaway(4); err != io.ErrShortWrite {
		t.Fatal("retried after failure", err)
	}
	select {
	case <-session.Done():
	case <-time.After(time.Second):
		t.Fatal("write failure did not stop session")
	}
	transport.check(t, InternalError)
}
func TestSessionStopJoinsGoawayWrite(t *testing.T) {
	control := &fakeStream{block: true, entered: make(chan struct{}), interrupt: make(chan struct{})}
	session, transport := sendingSession(t, false, control)
	result := make(chan error, 1)
	go func() { result <- session.SendGoaway(8) }()
	select {
	case <-control.entered:
	case <-time.After(time.Second):
		t.Fatal("control write did not block")
	}
	cause := errors.New("drain deadline")
	stopped := make(chan struct{})
	go func() { session.Stop(cause); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop stranded")
	}
	if err := <-result; err != cause {
		t.Fatal(err)
	}
	transport.mu.Lock()
	aborts := transport.aborts
	transport.mu.Unlock()
	if aborts != 0 {
		t.Fatal("local stop selected protocol error")
	}
}

func TestSessionAcceptPreservesDispatcherCause(t *testing.T) {
	parent, stop := context.WithCancelCause(context.Background())
	defer stop(context.Canceled)
	dispatcher, fail := context.WithCancelCause(parent)
	cause := &h3.Error{Code: h3.StreamCreationError, Reason: "duplicate critical stream"}
	fail(cause)
	transport := sessionIncoming{IncomingTransport: &dispatchTransport{streams: make(chan ReceiveStream)}, cancel: stop}
	if _, err := transport.AcceptUni(dispatcher); err != context.Canceled {
		t.Fatal(err)
	}
	if context.Cause(parent) != cause {
		t.Fatal("lost dispatcher cause", context.Cause(parent))
	}
}
