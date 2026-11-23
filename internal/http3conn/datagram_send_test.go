package http3conn

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	h3 "github.com/getlantern/photon/internal/http3wire"
)

type datagramSinkFunc func(context.Context, []byte) error

func (f datagramSinkFunc) SendDatagramContext(ctx context.Context, p []byte) error { return f(ctx, p) }
func datagramSendSession(t *testing.T, sink DatagramSink) (*Session, *DatagramRequest, context.CancelCauseFunc) {
	t.Helper()
	d, _ := NewDatagramDispatch(2, 2, 16)
	r, _ := d.Register(4, true)
	enabled := []h3.Setting{{ID: h3.H3Datagram, Value: 1}}
	settings, _ := NewSettingsState(false, enabled)
	if err := settings.ApplyPeer(enabled); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(context.Canceled) })
	return &Session{ctx: ctx, settings: settings, datagrams: d, datagramSender: sink}, r, cancel
}
func TestHTTPDatagramSendPermissions(t *testing.T) {
	var packets [][]byte
	s, r, _ := datagramSendSession(t, datagramSinkFunc(func(_ context.Context, p []byte) error {
		packets = append(packets, append([]byte(nil), p...))
		return nil
	}))
	r.CloseReceive() // Receive FIN must not close sending.
	if err := s.SendHTTPDatagram(context.Background(), r, []byte{7}); err != nil {
		t.Fatal(err)
	}
	if len(packets) != 1 || !bytes.Equal(packets[0], []byte{1, 7}) {
		t.Fatal(packets)
	}
	r.CloseSend()
	if err := s.SendHTTPDatagram(context.Background(), r, nil); !errors.Is(err, ErrDatagramSendClosed) {
		t.Fatal(err)
	}
	unsupported, _ := s.datagrams.Register(8, false)
	var request *DatagramRequestError
	if err := s.SendHTTPDatagram(context.Background(), unsupported, nil); !errors.As(err, &request) || request.ID != 8 {
		t.Fatal(err)
	}
	other, _ := NewDatagramDispatch(1, 1, 8)
	foreign, _ := other.Register(0, true)
	if err := s.SendHTTPDatagram(context.Background(), foreign, nil); !errors.Is(err, ErrRequestID) {
		t.Fatal(err)
	}
	s.settings, _ = NewSettingsState(false, []h3.Setting{{ID: h3.H3Datagram, Value: 1}})
	if err := s.SendHTTPDatagram(context.Background(), r, nil); !errors.Is(err, ErrHTTPDatagramsNotNegotiated) {
		t.Fatal(err)
	}
	if len(packets) != 1 {
		t.Fatal("rejected send reached transport")
	}
}
func TestHTTPDatagramBlockedSendCancellation(t *testing.T) {
	entered := make(chan struct{})
	s, r, cancel := datagramSendSession(t, datagramSinkFunc(func(ctx context.Context, _ []byte) error { close(entered); <-ctx.Done(); return ctx.Err() }))
	first := make(chan error, 1)
	go func() { first <- s.SendHTTPDatagram(context.Background(), r, nil) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("send not started")
	}
	// A caller behind another blocked send must still observe its own deadline.
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if err := s.SendHTTPDatagram(ctx, r, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	cause := errors.New("session ended")
	cancel(cause)
	select {
	case err := <-first:
		if err != cause {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("session failed to interrupt send")
	}
	r.CloseSend()
}

func TestHTTPDatagramCloseSendInterruptsBackpressure(t *testing.T) {
	entered := make(chan struct{})
	s, r, cancelSession := datagramSendSession(t, datagramSinkFunc(func(ctx context.Context, _ []byte) error { close(entered); <-ctx.Done(); return ctx.Err() }))
	sent := make(chan error, 1)
	go func() { sent <- s.SendHTTPDatagram(context.Background(), r, nil) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("send not started")
	}
	closed := make(chan struct{})
	go func() { r.CloseSend(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		cancelSession(context.Canceled)
		<-closed
		<-sent
		t.Fatal("CloseSend stranded behind transport backpressure")
	}
	if err := <-sent; !errors.Is(err, ErrDatagramSendClosed) {
		t.Fatal(err)
	}
	// Closing send alone leaves receive usable.
	if err := s.datagrams.Deliver([]byte{1, 9}); err != nil {
		t.Fatal(err)
	}
	if data, err := r.Receive(); err != nil || !bytes.Equal(data, []byte{9}) {
		t.Fatal(data, err)
	}
}
