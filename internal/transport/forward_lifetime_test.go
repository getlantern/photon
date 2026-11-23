package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

func forwardingRuntime(t *testing.T) *UDPServer {
	t.Helper()
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	e, err := NewClientOnlyEndpoint(8, 4)
	if err != nil {
		socket.Close()
		t.Fatal(err)
	}
	s, err := NewUDPServerBorrowed(socket, e)
	if err != nil {
		socket.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close(); socket.Close() })
	return s
}

func TestForwardRouteLifetime(t *testing.T) {
	for _, mode := range []string{"destination", "source", "explicit", "empty"} {
		t.Run(mode, func(t *testing.T) {
			source, destination := forwardingRuntime(t), forwardingRuntime(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			h, err := destination.handoffTarget(ctx)
			if err != nil {
				t.Fatal(err)
			}
			route, err := source.registerHandoffRoute(ctx, h, [][]byte{{1, 2, 3}})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "empty" {
				if err := source.updateHandoffRoute(ctx, route, nil); err != nil {
					t.Fatal(err)
				}
			}
			switch mode {
			case "destination", "empty":
				if err := destination.Close(); err != nil {
					t.Fatal(err)
				}
				if err := source.Wait(ctx, func(e *ServerEndpoint) (bool, error) {
					return len(e.router.foreignIDs) == 0 && len(e.router.forwardRegistrations) == 0, nil
				}); err != nil {
					t.Fatal("revocation did not wake owner", err)
				}
				if _, err := source.registerHandoffRoute(ctx, h, [][]byte{{1, 2, 3}}); !errors.Is(err, ErrEndpointClosed) {
					t.Fatal("closed destination registered", err)
				}
			case "source":
				if err := source.Close(); err != nil {
					t.Fatal(err)
				}
			case "explicit":
				for range 2 {
					if err := source.closeHandoffRoute(ctx, route); err != nil {
						t.Fatal(err)
					}
				}
				replacement, err := source.registerHandoffRoute(ctx, h, [][]byte{{1, 2, 3}})
				if err != nil {
					t.Fatal("CID not released", err)
				}
				if err := source.closeHandoffRoute(ctx, replacement); err != nil {
					t.Fatal(err)
				}
			}
			h.mu.Lock()
			retained := len(h.routes)
			h.mu.Unlock()
			if retained != 0 || !route.revoked.Load() {
				t.Fatal("retained target subscription")
			}
			if mode != "source" {
				if err := source.updateHandoffRoute(ctx, route, [][]byte{{9}}); !errors.Is(err, ErrEndpointClosed) {
					t.Fatal("stale route resurrected", err)
				}
			}
		})
	}
}

func TestForwardRouteReciprocalShutdown(t *testing.T) {
	for range 16 {
		a, b := forwardingRuntime(t), forwardingRuntime(t)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		ah, err := a.handoffTarget(ctx)
		if err != nil {
			t.Fatal(err)
		}
		bh, err := b.handoffTarget(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = a.registerHandoffRoute(ctx, bh, [][]byte{{1}}); err != nil {
			t.Fatal(err)
		}
		if _, err = b.registerHandoffRoute(ctx, ah, [][]byte{{2}}); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Go(func() { a.Close() })
		wg.Go(func() { b.Close() })
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-ctx.Done():
			t.Fatal("reciprocal shutdown deadlock")
		}
		cancel()
		if !ah.closed || !bh.closed || len(ah.routes) != 0 || len(bh.routes) != 0 {
			t.Fatal("shutdown retained subscriptions")
		}
	}
}

func TestForwardRouteUpdateDuringDestinationShutdown(t *testing.T) {
	for range 32 {
		source, destination := forwardingRuntime(t), forwardingRuntime(t)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		h, err := destination.handoffTarget(ctx)
		if err != nil {
			t.Fatal(err)
		}
		route, err := source.registerHandoffRoute(ctx, h, [][]byte{{1}})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() { destination.Close(); close(done) }()
		for n := range 32 {
			err := source.updateHandoffRoute(ctx, route, [][]byte{{byte(n + 2)}})
			if errors.Is(err, ErrEndpointClosed) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		select {
		case <-done:
		case <-ctx.Done():
			t.Fatal("destination shutdown stalled")
		}
		if err := source.Wait(ctx, func(e *ServerEndpoint) (bool, error) {
			return len(e.router.foreignIDs) == 0 && len(e.router.forwardRegistrations) == 0, nil
		}); err != nil {
			t.Fatal("concurrent update retained a revoked route", err)
		}
		if err := source.updateHandoffRoute(ctx, route, [][]byte{{99}}); !errors.Is(err, ErrEndpointClosed) {
			t.Fatal("closed route resurrected", err)
		}
		cancel()
	}
}
