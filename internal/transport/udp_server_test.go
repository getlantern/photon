package transport

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/wire"
)

func testUDPServer(t *testing.T) *UDPServer {
	t.Helper()
	_, serverTLS := testTLS(t)
	codec, _ := token.NewRetryTokens([32]byte{3}, nil, time.Minute)
	endpoint, err := NewServerEndpoint(Config{Version: wire.Version1, TLS: serverTLS}, codec, 4)
	if err != nil {
		t.Fatal(err)
	}
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewUDPServer(socket, endpoint)
	if err != nil {
		socket.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	return server
}
func TestUDPServerCancellationAndShutdown(t *testing.T) {
	s := testUDPServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	if err := s.Do(ctx, func(*ServerEndpoint) error { called = true; return nil }); !errors.Is(err, context.Canceled) || called {
		t.Fatal("cancelled command executed", err)
	}
	if _, _, err := s.Accept(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	accepted := make(chan error, 1)
	go func() { _, _, err := s.Accept(context.Background()); accepted <- err }()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-accepted:
		if !errors.Is(err, ErrEndpointClosed) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Accept stuck during shutdown")
	}
	if err := s.Do(context.Background(), func(*ServerEndpoint) error { return nil }); !errors.Is(err, ErrEndpointClosed) {
		t.Fatal(err)
	}
	select {
	case <-s.readerDone:
	default:
		t.Fatal("reader goroutine retained")
	}
}
func TestUDPServerSerializesCommands(t *testing.T) {
	s := testUDPServer(t)
	var count int
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Go(func() {
			if err := s.Do(context.Background(), func(*ServerEndpoint) error { count++; return nil }); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if count != 32 {
		t.Fatal(count)
	}
}

func TestUDPServerWaitNotifications(t *testing.T) {
	s := testUDPServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	const n = 16
	var calls atomic.Int32
	first := make(chan struct{}, n)
	ready := false // Only accessed by the owner goroutine.
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Go(func() {
			initial := true
			err := s.Wait(ctx, func(*ServerEndpoint) (bool, error) {
				calls.Add(1)
				if initial {
					first <- struct{}{}
					initial = false
				}
				return ready, nil
			})
			if err != nil {
				t.Error(err)
			}
		})
	}
	for i := 0; i < n; i++ {
		select {
		case <-first:
		case <-ctx.Done():
			t.Fatal("waiters did not start")
		}
	}
	// A read-only owner barrier must not itself trigger any readiness retry.
	if _, err := s.invoke(ctx, func(*ServerEndpoint) error { return nil }, false); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != n {
		t.Fatal("blocked waiters woke each other", calls.Load())
	}
	if err := s.Do(ctx, func(*ServerEndpoint) error { ready = true; return nil }); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if calls.Load() != 2*n {
		t.Fatal("unexpected retry count", calls.Load())
	}
}

func TestUDPServerWaitCancellationAndShutdown(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		s := testUDPServer(t)
		ctx, cancel := context.WithCancel(context.Background())
		entered := make(chan struct{})
		result := make(chan error, 1)
		go func() {
			result <- s.Wait(ctx, func(*ServerEndpoint) (bool, error) { close(entered); return false, nil })
		}()
		<-entered
		if shutdown {
			s.Close()
		} else {
			cancel()
		}
		select {
		case err := <-result:
			want := error(context.Canceled)
			if shutdown {
				want = ErrEndpointClosed
			}
			if !errors.Is(err, want) {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("blocked wait did not exit")
		}
		cancel()
	}
}

func TestDedicatedClientRuntimeShutdown(t *testing.T) {
	client, _ := testPair(t, wire.Version1, streamParameters())
	peer := netip.MustParseAddrPort("127.0.0.1:9999")
	endpoint, err := NewClientEndpoint(client, peer)
	if err != nil {
		t.Fatal(err)
	}
	// A client endpoint never allocates a server for an unknown datagram.
	if c, out, err := endpoint.HandleDatagram(context.Background(), make([]byte, 1200), peer, time.Now()); c != nil || out != nil || err != nil {
		t.Fatal(c, out, err)
	}
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewUDPServer(socket, endpoint)
	if err != nil {
		socket.Close()
		t.Fatal(err)
	}
	defer runtime.Close()
	if err := runtime.Do(context.Background(), func(e *ServerEndpoint) error { e.CloseWhenIdle(); e.Remove(client); return nil }); err != nil && !errors.Is(err, ErrEndpointClosed) {
		t.Fatal(err)
	}
	select {
	case <-runtime.Done():
	case <-time.After(time.Second):
		t.Fatal("owner remained alive")
	}
	select {
	case <-runtime.readerDone:
	case <-time.After(time.Second):
		t.Fatal("reader remained alive")
	}
}

func TestUDPServerLazyNotifications(t *testing.T) {
	s := testUDPServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	invoke := func(outcome error, notify bool) serverResult {
		t.Helper()
		result, err := s.invoke(ctx, func(*ServerEndpoint) error { return outcome }, notify)
		if !errors.Is(err, outcome) {
			t.Fatal(err)
		}
		return result
	}
	if got := invoke(nil, true).changed; got != nil {
		t.Fatal("successful command subscribed to notifications")
	}
	first := invoke(errOperationWouldBlock, true).changed
	accept := invoke(ErrAcceptWouldBlock, false).changed
	if first == nil || accept != first {
		t.Fatal("blocked operations must share the current notification")
	}
	select {
	case <-first:
		t.Fatal("blocked operations woke each other")
	default:
	}
	invoke(nil, true)
	select {
	case <-first:
	default:
		t.Fatal("completed command failed to wake registered operations")
	}
	next := invoke(errOperationWouldBlock, true).changed
	if next == nil || next == first {
		t.Fatal("new waiter needs a fresh notification")
	}
	select {
	case <-next:
		t.Fatal("new notification already closed")
	default:
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-next:
	default:
		t.Fatal("shutdown failed to wake registered operation")
	}
}
