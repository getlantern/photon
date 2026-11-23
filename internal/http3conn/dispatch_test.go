package http3conn

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/http3wire"
)

type dispatchTransport struct {
	streams chan ReceiveStream
	mu      sync.Mutex
	code    uint64
	aborts  int
}

func (t *dispatchTransport) AcceptUni(ctx context.Context) (ReceiveStream, error) {
	select {
	case s := <-t.streams:
		return s, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (t *dispatchTransport) Abort(code uint64, _ string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.code = code
	t.aborts++
	return nil
}
func (t *dispatchTransport) check(tst *testing.T, code uint64) {
	tst.Helper()
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.code != code || t.aborts != 1 {
		tst.Fatal(t.code, t.aborts)
	}
}
func receive(id int, typ byte) *fakeReceive {
	return &fakeReceive{Reader: bytes.NewReader([]byte{typ}), id: quic.StreamID(id)}
}
func TestDispatchCancellationJoins(t *testing.T) {
	transport := &dispatchTransport{streams: make(chan ReceiveStream, 3)}
	for i, typ := range []byte{0, 2, 3} {
		transport.streams <- receive(3+4*i, typ)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{}, 3)
	var active atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- RunIncoming(ctx, transport, true, 3, func(ctx context.Context, _ uint64, _ ReceiveStream) error {
			active.Add(1)
			defer active.Add(-1)
			entered <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	for i := 0; i < 3; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("handler missing")
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || active.Load() != 0 {
			t.Fatal(err, active.Load())
		}
	case <-time.After(time.Second):
		t.Fatal("workers not joined")
	}
	if transport.aborts != 0 {
		t.Fatal("parent cancellation selected protocol error")
	}
}
func TestDispatchCriticalReturnAndDuplicate(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		transport := &dispatchTransport{streams: make(chan ReceiveStream, 2)}
		transport.streams <- receive(3, 0)
		if duplicate {
			transport.streams <- receive(7, 0)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := RunIncoming(ctx, transport, true, 3, func(ctx context.Context, _ uint64, _ ReceiveStream) error {
			if duplicate {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		})
		cancel()
		code := http3wire.ClosedCriticalStream
		if duplicate {
			code = http3wire.StreamCreationError
		}
		var protocol *http3wire.Error
		if !errors.As(err, &protocol) || protocol.Code != code {
			t.Fatal(err)
		}
		transport.check(t, code)
	}
}
func TestDispatchWorkerLimit(t *testing.T) {
	transport := &dispatchTransport{streams: make(chan ReceiveStream, 4)}
	for i := 0; i < 4; i++ {
		transport.streams <- receive(3+4*i, 1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := RunIncoming(ctx, transport, true, 3, func(ctx context.Context, _ uint64, _ ReceiveStream) error { <-ctx.Done(); return ctx.Err() })
	var protocol *http3wire.Error
	if !errors.As(err, &protocol) || protocol.Code != http3wire.ExcessiveLoad {
		t.Fatal(err)
	}
	transport.check(t, http3wire.ExcessiveLoad)
}

type handlerBlockedStream struct {
	fakeReceive
	prefixed bool
}

func (s *handlerBlockedStream) Read(p []byte) (int, error) {
	if !s.prefixed {
		s.prefixed = true
		p[0] = 2
		return 1, nil
	}
	return s.fakeReceive.Read(p)
}
func TestDispatchCancellationInterruptsHandlerRead(t *testing.T) {
	stream := &handlerBlockedStream{fakeReceive: fakeReceive{id: 3, entered: make(chan struct{}), interrupt: make(chan struct{})}}
	transport := &dispatchTransport{streams: make(chan ReceiveStream, 1)}
	transport.streams <- stream
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- RunIncoming(ctx, transport, true, 3, func(_ context.Context, _ uint64, s ReceiveStream) error {
			var b [1]byte
			_, err := s.Read(b[:])
			return err
		})
	}()
	select {
	case <-stream.entered:
	case <-time.After(time.Second):
		t.Fatal("handler read missing")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked read not interrupted")
	}
}

func TestDispatchPreservesPeerConnectionClose(t *testing.T) {
	transport := &dispatchTransport{streams: make(chan ReceiveStream, 1)}
	transport.streams <- receive(2, 0)
	peerClose := &quic.PeerCloseError{Application: true, Code: 0x100, Reason: "drained"}
	err := RunIncoming(context.Background(), transport, false, 3, func(context.Context, uint64, ReceiveStream) error { return peerClose })
	if !errors.Is(err, peerClose) {
		t.Fatal("peer close relabeled", err)
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if transport.aborts != 0 {
		t.Fatal("connection close treated as HTTP3 failure", transport.code)
	}
}
