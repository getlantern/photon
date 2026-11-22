package transport

import (
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestForeignResetRouting(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(version.String(), func(t *testing.T) {
			e, c, source, writer, route := endpointForeignFixture(t, version)
			if err := e.bindForeignWriter(c, writer, route); err != nil {
				t.Fatal(err)
			}
			state := c.multipath.paths[1]
			app := state.application
			app.network.local = route.local // established foreign binding
			peer := app.network.remote
			current := state.peer.active[state.peer.current]
			current.used, current.hasToken = true, true
			current.token = [16]byte{0xaa, 0xbb, 0xcc}
			state.peer.active[state.peer.current] = current
			if _, err := e.router.publishForwardCIDs(c, nil); err != nil {
				t.Fatal(err)
			}
			source.applyForwardUpdates()
			packet := resetDatagram(current.token)
			if source.forwardRoute(packet) != nil {
				t.Fatal("fixture accidentally matched a CID")
			}
			if source.forwardResetRoute(packet, route.local, peer) != route {
				t.Fatal("reset did not select foreign owner")
			}
			first := route.resetHints.Load()
			route.publishResetHints(c)
			if route.resetHints.Load() != first {
				t.Fatal("unchanged hint republished")
			}
			bad := append([]byte(nil), packet...)
			bad[len(bad)-1] ^= 1
			for _, data := range [][]byte{bad, packet[:20]} {
				if source.forwardResetRoute(data, route.local, peer) != nil {
					t.Fatal("invalid reset routed")
				}
			}
			long := append([]byte{0x80}, packet[1:]...)
			if source.forwardResetRoute(long, route.local, peer) != route {
				t.Fatal("long-header reset lost")
			}
			if source.forwardResetRoute(packet, route.local, netip.MustParseAddrPort("192.0.2.99:443")) != nil {
				t.Fatal("unknown peer routed")
			}
			if source.forwardResetRoute(packet, netip.MustParseAddrPort("192.0.2.1:446"), peer) != nil {
				t.Fatal("wrong socket routed")
			}
			// Token hints do not authorize unused tokens, even on a known tuple.
			current.used = false
			state.peer.active[state.peer.current] = current
			route.publishResetHints(c)
			if source.forwardResetRoute(packet, route.local, peer) != nil {
				t.Fatal("unused token published")
			}
			e.handleForwardedDatagram(receivedUDP{data: packet, local: route.local, peer: peer, route: route, forwarded: true}, time.Unix(101, 0))
			if c.Draining() || c.closed {
				t.Fatal("unused token accepted")
			}
			current.used = true
			state.peer.active[state.peer.current] = current
			route.publishResetHints(c)
			ambiguous := &handoffRoute{router: source, target: route.target, local: route.local}
			ambiguous.resetHints.Store(route.resetHints.Load())
			source.forwardRegistrations[ambiguous] = struct{}{}
			if source.forwardResetRoute(packet, route.local, peer) != nil {
				t.Fatal("ambiguous token selected an owner")
			}
			ambiguous.revoked.Store(true)
			if source.forwardResetRoute(packet, route.local, peer) != route {
				t.Fatal("revoked hint interfered with live owner")
			}
			delete(source.forwardRegistrations, ambiguous)
			// A once-established socket is no longer reset authority after a switch.
			app.network.local = netip.MustParseAddrPort("192.0.2.1:446")
			e.handleForwardedDatagram(receivedUDP{data: packet, local: route.local, peer: peer, route: route, forwarded: true}, time.Unix(101, 0))
			if c.Draining() || c.closed {
				t.Fatal("old tuple accepted reset")
			}
			route.publishResetHints(c)
			if source.forwardResetRoute(packet, route.local, peer) != nil {
				t.Fatal("inactive socket retained reset hint")
			}
			app.network.local = route.local
			route.publishResetHints(c)
			// A stale hint must not authorize a reset after peer token retirement.
			current.hasToken = false
			state.peer.active[state.peer.current] = current
			e.handleForwardedDatagram(receivedUDP{data: packet, local: route.local, peer: peer, route: route, forwarded: true}, time.Unix(101, 0))
			if c.Draining() || c.closed {
				t.Fatal("stale routing hint authorized reset")
			}
			route.publishResetHints(c)
			if source.forwardResetRoute(packet, route.local, peer) != nil {
				t.Fatal("retired token hint retained")
			}
			current.hasToken = true
			state.peer.active[state.peer.current] = current
			route.publishResetHints(c)
			e.handleForwardedDatagram(receivedUDP{data: packet, local: route.local, peer: peer, route: route, forwarded: true}, time.Unix(101, 0))
			if !c.Draining() {
				t.Fatal("forwarded reset not accepted on established tuple", c.Err())
			}
		})
	}
}

func TestForeignFirstUseResetPublication(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(version.String(), func(t *testing.T) {
			e, c, source, writer, route := endpointForeignFixture(t, version)
			if err := e.bindForeignWriter(c, writer, route); err != nil {
				t.Fatal(err)
			}
			state := c.multipath.paths[1]
			state.application.network.local = route.local
			remote := state.application.network.remote
			peer := state.peer.active[state.peer.current]
			peer.hasToken, peer.used = true, false
			peer.token = [16]byte{0x42}
			state.peer.active[state.peer.current] = peer
			route.publishResetHints(c)
			reset := resetDatagram(peer.token)
			if source.forwardResetRoute(reset, route.local, remote) != nil {
				t.Fatal("unused token published")
			}
			ticket, err := writer.reserve(route)
			if err != nil {
				t.Fatal(err)
			}
			peer.used = true // packet construction has just committed its first use
			state.peer.active[state.peer.current] = peer
			pending, err := beginForeignWrite(ticket, &Outbound{Conn: c, Data: []byte{0x40}, Peer: remote, Local: route.local}, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer pending.release()
			if source.forwardResetRoute(reset, route.local, remote) != route {
				t.Fatal("write visible before token publication")
			}
			if len(writer.writes) != 1 {
				t.Fatal("write not submitted")
			}
		})
	}
}
