package transport

import (
	"bytes"
	"errors"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func TestPacketRouteReservationBeforeMigrationCommit(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			c, peer := testPair(t, v, streamParameters())
			drivePair(t, c, peer, false, false)
			old := netip.MustParseAddrPort("192.0.2.1:443")
			next := netip.MustParseAddrPort("192.0.2.1:445")
			remote := netip.MustParseAddrPort("192.0.2.2:443")
			if err := c.activateBasePath(old, remote); err != nil {
				t.Fatal(err)
			}
			if err := peer.activateBasePath(remote, old); err != nil {
				t.Fatal(err)
			}
			c.peerParams.DisableActiveMigration = false
			now := time.Unix(101, 0)
			candidate, err := c.prepareLocalPathProbe(0, next, now)
			if err != nil {
				t.Fatal(err)
			}
			router, _ := NewRouter(8, 4)
			writer := newPacketWriter(router)
			defer writer.close()
			route := &handoffRoute{router: router, local: next, target: newPacketHandoff()}
			scope := &packetRouteReservations{locals: map[netip.AddrPort]bool{old: true}, foreign: map[netip.AddrPort]foreignPacketRoute{next: {writer, route}}}
			for range cap(writer.writes) {
				if _, err := writer.offer(route, []byte{1}, remote, recovery.NotECT, nil); err != nil {
					t.Fatal(err)
				}
			}
			app := c.multipath.paths[0].application
			pn, seals, totals, credit := app.space.nextPN, c.sealed, c.sentTotals, candidate.budget
			c.packetRoutes = scope
			data, _, err := c.nextLocalMigrationProbe(0, now)
			c.packetRoutes = nil
			if err != nil || data != nil || !errors.Is(scope.err, errOperationWouldBlock) {
				t.Fatal("full route was not blocked", err, scope.err)
			}
			if app.space.nextPN != pn || c.sealed != seals || c.sentTotals != totals || candidate.budget != credit || len(candidate.validation.probes) != 0 || scope.ticket != nil {
				t.Fatal("queue pressure committed probe state")
			}
			id, err := c.OpenStream(false)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.WriteStream(id, []byte("old path remains usable"), true); err != nil {
				t.Fatal(err)
			}
			_, tuple, packet, ticket, err := c.nextBoundPacketWithRoutes(now, scope)
			if err != nil || len(packet) == 0 || tuple.local != old || ticket != nil || c.packetRoutes != nil {
				t.Fatal("full foreign queue blocked old path", err)
			}
			decoded, err := peer.authenticateMultipathPacket(packet, now)
			if err != nil || decoded == nil {
				t.Fatal(err)
			}
			streamFound := false
			for _, frame := range decoded.packet.frames {
				if _, ok := frame.(wire.StreamFrame); ok {
					streamFound = true
				}
			}
			if !streamFound {
				t.Fatal("old path did not carry stream")
			}
			request := <-writer.writes
			request.complete(1, nil)
			writer.capacityAvailable()
			_, tuple, packet, ticket, err = c.nextBoundPacketWithRoutes(now.Add(time.Millisecond), scope)
			if err != nil || len(packet) == 0 || tuple.local != next || ticket == nil || scope.ticket != nil || c.packetRoutes != nil {
				t.Fatal("missing reserved foreign output", err)
			}
			if _, err := writer.offer(route, []byte{2}, remote, recovery.NotECT, nil); !errors.Is(err, errOperationWouldBlock) {
				t.Fatal("competing writer stole ticket", err)
			}
			out := &Outbound{Conn: c, Local: tuple.local, Peer: tuple.remote, Data: packet}
			pending, err := beginForeignWrite(ticket, out, nil)
			if err != nil {
				t.Fatal(err)
			}
			for range cap(writer.writes) - 1 {
				request := <-writer.writes
				request.complete(1, nil)
			}
			actual := <-writer.writes
			// A nil result is a silent discard, not a successful authentication.
			probe, err := peer.authenticateMultipathPacket(actual.data, now)
			if err != nil || probe == nil {
				t.Fatal("reserved probe not decryptable", err)
			}
			challenged := false
			for _, frame := range probe.packet.frames {
				if _, ok := frame.(wire.PathChallengeFrame); ok {
					challenged = true
				}
			}
			if !challenged || !bytes.Equal(actual.data, packet) {
				t.Fatal("reserved write did not carry the constructed probe")
			}
			actual.complete(len(actual.data), nil)
			if _, result, ok := pending.poll(); !ok || result.err != nil || c.pendingWrite != nil {
				t.Fatal("reserved packet did not complete", result)
			}
			// A construction failure after reservation must release its ticket.
			app.space.nextPN = wire.MaxVarint
			_, _, _, failedTicket, err := c.nextBoundPacketWithRoutes(candidate.validation.next, scope)
			if err == nil || failedTicket != nil || len(writer.reserved) != 0 || c.packetRoutes != nil {
				t.Fatal("failed construction retained reservation", err)
			}

		})
	}
}

func TestPacketRouteReservationCleanup(t *testing.T) {
	r, _ := NewRouter(8, 4)
	w := newPacketWriter(r)
	defer w.close()
	local := netip.MustParseAddrPort("192.0.2.1:443")
	route := &handoffRoute{router: r, local: local}
	tuple := pathTuple{local, netip.MustParseAddrPort("192.0.2.2:443")}
	scope := &packetRouteReservations{foreign: map[netip.AddrPort]foreignPacketRoute{local: {w, route}}}
	if !scope.reserve(tuple) || len(w.reserved) != 1 {
		t.Fatal("missing reservation")
	}
	if !scope.reserve(tuple) || len(w.reserved) != 1 {
		t.Fatal("reselection leaked reservation")
	}
	wrong := tuple
	wrong.remote = netip.MustParseAddrPort("192.0.2.2:444")
	if ticket, err := scope.take(wrong); ticket != nil || !errors.Is(err, errPathBinding) || len(w.reserved) != 0 {
		t.Fatal("mismatched tuple retained ticket", err)
	}
	if !scope.reserve(tuple) {
		t.Fatal("could not reserve again")
	}
	scope.cancel()
	scope.cancel()
	if len(w.reserved) != 0 {
		t.Fatal("cancel leaked")
	}
	route.revoke()
	if scope.reserve(tuple) || !errors.Is(scope.err, ErrEndpointClosed) {
		t.Fatal("revoked route reserved", scope.err)
	}
}

func TestPacketRouteReservationEstablishedOutput(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			_, c, _, _ := ownedOutboundFixture(t, v, "stream")
			app := c.multipath.paths[1].application
			tuple := *app.network
			router, _ := NewRouter(8, 4)
			writer := newPacketWriter(router)
			defer writer.close()
			route := &handoffRoute{router: router, local: tuple.local, target: newPacketHandoff()}
			scope := &packetRouteReservations{foreign: map[netip.AddrPort]foreignPacketRoute{tuple.local: {writer, route}}}
			id, err := c.OpenStream(false)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.WriteStream(id, []byte("foreign established path"), true); err != nil {
				t.Fatal(err)
			}
			for range cap(writer.writes) {
				if _, err := writer.offer(route, []byte{1}, tuple.remote, recovery.NotECT, nil); err != nil {
					t.Fatal(err)
				}
			}
			pn, flight, totals := app.space.nextPN, app.sent.BytesInFlight, c.sentTotals
			_, _, data, ticket, err := c.nextBoundPacketWithRoutes(time.Unix(102, 0), scope)
			if err != nil || len(data) != 0 || ticket != nil || app.space.nextPN != pn || app.sent.BytesInFlight != flight || c.sentTotals != totals {
				t.Fatal("blocked established route consumed send", err)
			}
			if !app.routeBlocked || !c.multipathRecoveryDeadline().IsZero() {
				t.Fatal("blocked source left a recovery wakeup")
			}
			if err := c.expireMultipathRecovery(time.Unix(103, 0)); err != nil || app.sent.BytesInFlight != flight {
				t.Fatal("blocked source advanced recovery", err)
			}
			request := <-writer.writes
			request.complete(1, nil)
			writer.capacityAvailable()
			_, got, data, ticket, err := c.nextBoundPacketWithRoutes(time.Unix(102, 0), scope)
			if err != nil || len(data) == 0 || ticket == nil || got != tuple || app.space.nextPN != pn+1 {
				t.Fatal("established route not resumed", err)
			}
			ticket.cancel()
			if len(writer.reserved) != 0 || c.packetRoutes != nil {
				t.Fatal("selection leaked state")
			}
		})
	}
}

func TestPacketRouteReservationUnusedCloseTicket(t *testing.T) {
	_, c, _, _ := ownedOutboundFixture(t, wire.Version1, "stream")
	tuple := *c.multipath.paths[1].application.network
	r, _ := NewRouter(8, 4)
	w := newPacketWriter(r)
	defer w.close()
	route := &handoffRoute{router: r, local: tuple.local}
	scope := &packetRouteReservations{foreign: map[netip.AddrPort]foreignPacketRoute{tuple.local: {w, route}}}
	now := time.Unix(102, 0)
	if err := c.CloseWithError(0, "done", now); err != nil {
		t.Fatal(err)
	}
	_, _, data, ticket, err := c.nextBoundPacketWithRoutes(now, scope)
	if err != nil || len(data) == 0 || ticket == nil {
		t.Fatal("close emission not reserved", err)
	}
	ticket.cancel()
	// The cached close has no response credit at this instant. The pre-emission
	// reservation must be canceled when the closing sender produces no output.
	_, _, data, ticket, err = c.nextBoundPacketWithRoutes(now, scope)
	if err != nil || len(data) != 0 || ticket != nil || len(w.reserved) != 0 || c.packetRoutes != nil {
		t.Fatal("empty close emission leaked capacity", err)
	}
}
