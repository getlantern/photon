package transport

import (
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestForwardConnectionClosingLifetime(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, draining := range []bool{false, true} {
			c, peer := testPair(t, version)
			drivePair(t, c, peer, false, false)
			destination, _ := NewRouter(len(c.config.LocalCID), 8)
			if err := destination.Add(c, netip.MustParseAddrPort("192.0.2.1:443")); err != nil {
				t.Fatal(err)
			}
			h := newPacketHandoff()
			defer h.close()
			destination.handoff = h
			source, _ := NewRouter(8, 8)
			route := &handoffRoute{router: source, target: h, wake: make(chan struct{}, 1)}
			if !h.subscribe(route) {
				t.Fatal("subscribe")
			}
			if err := source.setForwardCIDs(route, [][]byte{c.config.LocalCID}); err != nil {
				t.Fatal(err)
			}
			source.forwardRegistrations = map[*handoffRoute]struct{}{route: {}}
			if err := h.bindConnection(route, c); err != nil {
				t.Fatal(err)
			}
			// An unrelated subscription must survive this connection's removal.
			other := &handoffRoute{router: source, target: h}
			if !h.subscribe(other) {
				t.Fatal("subscribe unrelated")
			}
			now := time.Unix(200, 0)
			if draining {
				c.beginDraining(ErrPeerClosed, now)
			} else if err := c.CloseWithError(0, "done", now); err != nil {
				t.Fatal(err)
			}
			if err := destination.Sync(c); err != nil {
				t.Fatal(err)
			}
			if route.revoked.Load() || len(source.foreignIDs) == 0 {
				t.Fatal("closing/draining lost forwarding")
			}
			if err := c.HandleTimeout(now.Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			if !c.closed {
				t.Fatal("connection not closed at deadline")
			}
			if err := destination.Sync(c); err != nil {
				t.Fatal(err)
			}
			source.pruneForwardRoutes()
			if !route.revoked.Load() || len(source.foreignIDs) != 0 || len(source.forwardRegistrations) != 0 {
				t.Fatal("final removal retained route")
			}
			if len(h.connections) != 0 || len(h.routes) != 1 || other.revoked.Load() {
				t.Fatal("connection cleanup affected unrelated subscription")
			}
		}
	}
}

func TestForwardConnectionBindingRuntime(t *testing.T) {
	source, destination, unrelated := forwardingRuntime(t), forwardingRuntime(t), forwardingRuntime(t)
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
	c, _ := testPair(t, wire.Version1)
	if err := destination.bindHandoffConnection(ctx, route, c); !errors.Is(err, ErrEndpointClosed) {
		t.Fatal("unregistered connection accepted", err)
	}
	if err := destination.Do(ctx, func(e *ServerEndpoint) error { return e.router.Add(c, netip.MustParseAddrPort("192.0.2.1:443")) }); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := destination.bindHandoffConnection(ctx, route, c); err != nil {
			t.Fatal(err)
		}
	}
	if err := unrelated.bindHandoffConnection(ctx, route, c); err == nil {
		t.Fatal("wrong destination accepted")
	}
	if err := destination.Do(ctx, func(e *ServerEndpoint) error { e.router.Remove(c); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := source.Wait(ctx, func(e *ServerEndpoint) (bool, error) {
		return len(e.router.foreignIDs) == 0 && len(e.router.forwardRegistrations) == 0, nil
	}); err != nil {
		t.Fatal("connection removal failed to wake source", err)
	}
	if err := destination.bindHandoffConnection(ctx, route, c); !errors.Is(err, ErrEndpointClosed) {
		t.Fatal("removed connection accepted", err)
	}
}

func TestForwardConnectionUnsubscribe(t *testing.T) {
	h := newPacketHandoff()
	defer h.close()
	r, _ := NewRouter(8, 8)
	a, b := &Conn{}, &Conn{}
	route := &handoffRoute{router: r, target: h}
	h.subscribe(route)
	if err := h.bindConnection(route, a); err != nil {
		t.Fatal(err)
	}
	if err := h.bindConnection(route, b); err == nil {
		t.Fatal("rebound to different connection")
	}
	h.unsubscribe(route)
	if len(h.connections) != 0 || len(h.routes) != 0 {
		t.Fatal("unsubscribe retained connection")
	}
	if err := h.bindConnection(route, a); !errors.Is(err, ErrEndpointClosed) {
		t.Fatal("unsubscribed route rebound", err)
	}
}
