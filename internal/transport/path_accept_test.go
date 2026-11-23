package transport

import (
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestMultipathServerAcceptAndValidate(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := sendingPathPair(t, version)
		ca := netip.MustParseAddrPort("192.0.2.1:443")
		sa := netip.MustParseAddrPort("192.0.2.2:444")
		for _, c := range []*Conn{client, server} {
			c.multipath.paths[1].application = nil
		}
		if err := client.bindPreparedPath(0, ca, sa); err != nil {
			t.Fatal(err)
		}
		if err := server.bindPreparedPath(0, sa, ca); err != nil {
			t.Fatal(err)
		}
		if err := client.initiateMultipathPath(1, ca, sa); err != nil {
			t.Fatal(err)
		}
		now := time.Unix(101, 0)
		for i := 0; i < 16; i++ {
			for _, pair := range [][2]*Conn{{client, server}, {server, client}} {
				_, tuple, b, err := pair[0].nextBoundMultipathPacket(now)
				if err != nil {
					t.Fatal(err)
				}
				if b == nil {
					continue
				}
				if err := pair[1].handleBoundMultipathPacket(b, tuple.remote, tuple.local, now); err != nil {
					t.Fatal(err)
				}
			}
			now = now.Add(time.Millisecond)
		}
		for _, c := range []*Conn{client, server} {
			app := c.multipath.paths[1].application
			if app == nil || app.validation.state != PathValidated || !app.budget.Validated() {
				t.Fatal("path not accepted/validated")
			}
		}
	}
}

func TestMultipathServerAcceptanceAuthentication(t *testing.T) {
	client, server := sendingPathPair(t, wire.Version1)
	ca := netip.MustParseAddrPort("192.0.2.1:443")
	sa := netip.MustParseAddrPort("192.0.2.2:444")
	now := time.Unix(101, 0)
	client.multipath.paths[1].application.budget.Validate()
	// A first packet need not contain PATH_CHALLENGE.
	if err := client.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
		t.Fatal(err)
	}
	raw, err := client.nextPacketOnPath(1, now)
	if err != nil || raw == nil {
		t.Fatal(err)
	}
	state := server.multipath.paths[1]
	state.application = nil
	originalIdle := server.idle
	bad := append([]byte(nil), raw...)
	bad[len(bad)-1] ^= 1
	for i := 0; i < 3; i++ {
		if err := server.handleBoundMultipathPacket(bad, sa, ca, now); err != nil {
			t.Fatal(err)
		}
		if state.application != nil || server.idle != originalIdle {
			t.Fatal("forged packet established path")
		}
	}
	if err := server.handleBoundMultipathPacket(raw, sa, ca, now); err != nil {
		t.Fatal(err)
	}
	app := state.application
	if app == nil || app.network == nil || app.budget.Validated() || app.budget.credit != uint64(3*len(raw)) || app.validation.state != PathValidating {
		t.Fatal("acceptance skipped validation/credit")
	}
	queued := len(app.space.queue)
	if err := server.handleBoundMultipathPacket(raw, sa, ca, now); err != nil {
		t.Fatal(err)
	}
	if state.application != app || len(app.space.queue) != queued {
		t.Fatal("replay reinitialized path")
	}
}

func TestMultipathClientRejectsPeerInitiation(t *testing.T) {
	client, server := sendingPathPair(t, wire.Version2)
	server.multipath.paths[1].application.budget.Validate()
	now := time.Unix(101, 0)
	if err := server.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
		t.Fatal(err)
	}
	raw, err := server.nextPacketOnPath(1, now)
	if err != nil || raw == nil {
		t.Fatal(err)
	}
	client.multipath.paths[1].application = nil
	ca := netip.MustParseAddrPort("192.0.2.1:443")
	sa := netip.MustParseAddrPort("192.0.2.2:444")
	if err := client.handleBoundMultipathPacket(raw, ca, sa, now); err != nil {
		t.Fatal(err)
	}
	if client.multipath.paths[1].application != nil {
		t.Fatal("server initiated a path")
	}
}

func TestMultipathServerAcceptsReorderedInitiations(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := sendingPathPair(t, version)
		for _, c := range []*Conn{client, server} {
			c.multipath.paths[1].application = nil
			if _, err := c.multipath.issue(2); err != nil {
				t.Fatal(err)
			}
		}
		for _, pair := range [][2]*Conn{{client, server}, {server, client}} {
			for seq, cid := range pair[0].multipath.paths[2].local.active {
				f := wire.PathNewConnectionIDFrame{PathID: 2, NewConnectionIDFrame: wire.NewConnectionIDFrame{Sequence: seq, ConnectionID: cid.cid, StatelessResetToken: cid.token}}
				if _, err := pair[1].multipath.receiveNew(f); err != nil {
					t.Fatal(err)
				}
				pair[0].multipath.markSent(2, seq)
			}
		}
		local := netip.MustParseAddrPort("192.0.2.1:443")
		remote := netip.MustParseAddrPort("192.0.2.2:444")
		for _, id := range []uint64{1, 2} {
			if err := client.initiateMultipathPath(id, local, remote); err != nil {
				t.Fatal(err)
			}
		}
		now := time.Unix(101, 0)
		// Network delivery need not match the client's initiation order.
		for _, id := range []uint64{2, 1} {
			packet, err := client.nextPacketOnPath(id, now)
			if err != nil || packet == nil {
				t.Fatal(err)
			}
			if err := server.handleBoundMultipathPacket(packet, remote, local, now); err != nil {
				t.Fatal(err)
			}
			if server.multipath.paths[id].application == nil {
				t.Fatal("server rejected reordered path", id)
			}
		}
	}
}
