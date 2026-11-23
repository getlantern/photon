package transport

import (
	"errors"
	"net/netip"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func endpointForeignFixture(t *testing.T, v wire.Version) (*ServerEndpoint, *Conn, *Router, *packetWriter, *handoffRoute) {
	t.Helper()
	e, c := mtuWriteEndpoint(t, v)
	t.Cleanup(e.Close)
	h := newPacketHandoff()
	t.Cleanup(h.close)
	e.router.handoff = h
	e.publicationWake = h.changed
	source, _ := NewRouter(8, 8)
	route := updateTestRoute(t, source, h, nil)
	route.local = netip.MustParseAddrPort("192.0.2.1:445")
	if err := h.bindConnection(route, c); err != nil {
		t.Fatal(err)
	}
	writer := newPacketWriter(source)
	t.Cleanup(writer.close)
	return e, c, source, writer, route
}

func TestEndpointForeignRoutePublication(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(v.String(), func(t *testing.T) {
			e, c, source, writer, route := endpointForeignFixture(t, v)
			if err := e.bindForeignWriter(c, writer, route); err != nil {
				t.Fatal(err)
			}
			if err := e.bindForeignWriter(c, writer, route); err != nil || e.foreignWriterCount != 1 {
				t.Fatal("duplicate bind", err)
			}
			if e.locals[route.local] {
				t.Fatal("foreign socket became owned listener")
			}
			if scope := e.foreignPacketRoutes(c); len(scope.foreign) != 0 {
				t.Fatal("route eligible before source publication")
			}
			source.applyForwardUpdates()
			if scope := e.foreignPacketRoutes(c); scope.foreign[route.local].route != route {
				t.Fatal("acknowledged route missing")
			}
			// A new local CID invalidates the old source acknowledgement immediately.
			state := c.multipath.paths[1].local
			if _, err := state.issue(uint64(len(state.active) + 1)); err != nil {
				t.Fatal(err)
			}
			if scope := e.foreignPacketRoutes(c); len(scope.foreign) != 0 {
				t.Fatal("stale revision authorized output")
			}
			source.applyForwardUpdates()
			if scope := e.foreignPacketRoutes(c); scope.foreign[route.local].writer != writer {
				t.Fatal("new revision not authorized")
			}
			e.Remove(c)
			if e.foreignWriterCount != 0 || len(e.foreignWriters) != 0 || !route.revoked.Load() {
				t.Fatal("connection removal leaked foreign binding")
			}
		})
	}
}

func TestEndpointForeignRouteAuthorization(t *testing.T) {
	for _, mode := range []string{"writer", "target", "binding", "wildcard", "owned", "revoked", "closed-writer"} {
		t.Run(mode, func(t *testing.T) {
			e, c, _, writer, route := endpointForeignFixture(t, wire.Version1)
			switch mode {
			case "writer":
				r, _ := NewRouter(8, 2)
				writer = newPacketWriter(r)
				defer writer.close()
			case "target":
				route.target = newPacketHandoff()
				defer route.target.close()
			case "binding":
				route.target.mu.Lock()
				route.target.routes[route] = nil
				route.target.mu.Unlock()
			case "wildcard":
				route.local = netip.MustParseAddrPort("0.0.0.0:445")
			case "owned":
				route.local = e.local
			case "revoked":
				route.revoke()
			case "closed-writer":
				writer.close()
			}
			if err := e.bindForeignWriter(c, writer, route); err == nil || e.foreignWriterCount != 0 {
				t.Fatal("invalid route authorized", err)
			}
		})
	}
}

func TestEndpointForeignRouteRemovalAndCollision(t *testing.T) {
	for _, mode := range []string{"remove", "revoke", "close", "collision"} {
		t.Run(mode, func(t *testing.T) {
			e, c, source, writer, route := endpointForeignFixture(t, wire.Version1)
			if err := e.bindForeignWriter(c, writer, route); err != nil {
				t.Fatal(err)
			}
			if mode == "collision" {
				other := updateTestRoute(t, source, route.target, nil)
				other.local = route.local
				if err := route.target.bindConnection(other, c); err != nil {
					t.Fatal(err)
				}
				if err := e.bindForeignWriter(c, writer, other); !errors.Is(err, ErrRouteCollision) {
					t.Fatal(err)
				}
				if e.foreignWriters[c][route.local].route != route || e.foreignWriterCount != 1 {
					t.Fatal("collision replaced registration")
				}
				return
			}
			switch mode {
			case "remove":
				e.removeForeignWriter(c, route.local)
			case "revoke":
				route.revoke()
			case "close":
				writer.close()
			}
			if scope := e.foreignPacketRoutes(c); len(scope.foreign) != 0 || e.foreignWriterCount != 0 || c.closed || !route.revoked.Load() {
				t.Fatal("dead alternative retained or primary closed")
			}
		})
	}
}

func TestEndpointForeignRouteWriterCloseRace(t *testing.T) {
	for range 50 {
		e, c, source, writer, route := endpointForeignFixture(t, wire.Version1)
		if err := e.bindForeignWriter(c, writer, route); err != nil {
			t.Fatal(err)
		}
		e.foreignPacketRoutes(c)
		source.applyForwardUpdates()
		closed := make(chan struct{})
		go func() { writer.close(); close(closed) }()
		scope := e.foreignPacketRoutes(c)
		if binding, ok := scope.foreign[route.local]; ok {
			// A snapshot can race shutdown. Reservation must still reject a closed writer.
			<-closed
			if _, err := binding.writer.reserve(binding.route); !errors.Is(err, ErrEndpointClosed) {
				t.Fatal("closed snapshot remained usable", err)
			}
		} else {
			<-closed
		}
		e.foreignPacketRoutes(c)
		if e.foreignWriterCount != 0 || c.closed {
			t.Fatal("shutdown did not prune only alternative")
		}
	}
}

func TestEndpointForeignRoutePerSourceReadiness(t *testing.T) {
	e, c, first, writer, route := endpointForeignFixture(t, wire.Version1)
	if err := e.bindForeignWriter(c, writer, route); err != nil {
		t.Fatal(err)
	}
	second, _ := NewRouter(8, 8)
	other := updateTestRoute(t, second, route.target, nil)
	other.local = netip.MustParseAddrPort("192.0.2.1:446")
	if err := route.target.bindConnection(other, c); err != nil {
		t.Fatal(err)
	}
	otherWriter := newPacketWriter(second)
	defer otherWriter.close()
	if err := e.bindForeignWriter(c, otherWriter, other); err != nil {
		t.Fatal(err)
	}
	e.foreignPacketRoutes(c)
	first.applyForwardUpdates()
	scope := e.foreignPacketRoutes(c)
	if len(scope.foreign) != 1 || scope.foreign[route.local].route != route || !c.forwardingBlocked {
		t.Fatal("one source acknowledgement authorized another")
	}
	second.applyForwardUpdates()
	scope = e.foreignPacketRoutes(c)
	if len(scope.foreign) != 2 || c.forwardingBlocked {
		t.Fatal("both acknowledged sources not ready")
	}
	e.removeForeignWriter(c, route.local)
	if e.foreignWriterCount != 1 || e.foreignPacketRoutes(c).foreign[other.local].route != other || c.closed {
		t.Fatal("removal disturbed surviving route")
	}
}

func TestEndpointForeignRouteRejectedPublication(t *testing.T) {
	e, c, source, writer, route := endpointForeignFixture(t, wire.Version1)
	other, _ := testPair(t, wire.Version1)
	if err := source.Add(other, netip.MustParseAddrPort("192.0.2.3:443")); err != nil {
		t.Fatal(err)
	}
	if err := e.bindForeignWriter(c, writer, route); err != nil {
		t.Fatal(err)
	}
	e.foreignPacketRoutes(c)
	source.applyForwardUpdates()
	if scope := e.foreignPacketRoutes(c); len(scope.foreign) != 0 || c.closed || !c.forwardingBlocked {
		t.Fatal("failed publication authorized output or closed primary")
	}
	route.target.mu.Lock()
	failure := route.target.connections[c][route].err
	route.target.mu.Unlock()
	if !errors.Is(failure, ErrRouteCollision) {
		t.Fatal("fixture did not collide", failure)
	}
	e.removeForeignWriter(c, route.local)
	e.foreignPacketRoutes(c)
	if c.closed || c.forwardingBlocked || e.foreignWriterCount != 0 {
		t.Fatal("failed alternative removal did not restore publication")
	}
}
