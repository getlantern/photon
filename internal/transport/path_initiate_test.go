package transport

import (
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestMultipathClientInitiation(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := sendingPathPair(t, version)
		client.multipath.paths[1].application = nil // CID exchange precedes initiation.
		ca := netip.MustParseAddrPort("192.0.2.1:443")
		sa := netip.MustParseAddrPort("192.0.2.2:444")
		if err := client.bindPreparedPath(0, ca, sa); err != nil {
			t.Fatal(err)
		}
		if err := client.initiateMultipathPath(1, ca, sa); err != nil {
			t.Fatal(err)
		}
		app := client.multipath.paths[1].application
		if app.validation.state != PathValidating || !app.budget.Validated() || len(app.space.queue) != 1 || app.space.nextPN != 0 {
			t.Fatal("initiation state")
		}
		if err := client.initiateMultipathPath(1, ca, sa); !errors.Is(err, errPathInitiation) {
			t.Fatal("ID reused", err)
		}
		// A prepared server context exercises the bound receive/response path;
		// automatic first-packet acceptance is still a separate integration step.
		if err := server.bindPreparedPath(1, sa, ca); err != nil {
			t.Fatal(err)
		}
		now := time.Unix(101, 0)
		id, tuple, b, err := client.nextBoundMultipathPacket(now)
		if err != nil || id != 1 || len(b) != 1200 || tuple != (pathTuple{ca, sa}) {
			t.Fatal(id, len(b), err)
		}
		if err := server.handleBoundMultipathPacket(b, sa, ca, now); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			response, err := server.nextPacketOnPath(1, now.Add(time.Millisecond))
			if err != nil {
				t.Fatal(err)
			}
			if response == nil {
				break
			}
			if err := client.handleBoundMultipathPacket(response, ca, sa, now.Add(2*time.Millisecond)); err != nil {
				t.Fatal(err)
			}
		}
		if app.validation.state != PathValidated {
			t.Fatal("new path did not validate")
		}
	}
}

func TestMultipathInitiationPreconditionsAndRollback(t *testing.T) {
	ca := netip.MustParseAddrPort("192.0.2.1:443")
	sa := netip.MustParseAddrPort("192.0.2.2:444")
	for _, test := range []string{"server", "unsent CID", "missing peer CID", "over limit", "abandoned", "full queue"} {
		t.Run(test, func(t *testing.T) {
			client, server := sendingPathPair(t, wire.Version1)
			c := client
			if test == "server" {
				c = server
			}
			state := c.multipath.paths[1]
			state.application = nil
			switch test {
			case "unsent CID":
				clear(state.local.sent)
			case "missing peer CID":
				clear(state.peer.active)
			case "over limit":
				c.multipath.peerMax = 0
			case "abandoned":
				if _, err := c.multipath.beginAbandon(1); err != nil {
					t.Fatal(err)
				}
			case "full queue":
				c.queuedBytes = maxSendBytes
			}
			before := c.queuedBytes
			if err := c.initiateMultipathPath(1, ca, sa); err == nil {
				t.Fatal("accepted invalid initiation")
			}
			if state.application != nil || c.queuedBytes != before {
				t.Fatal("failed initiation changed state")
			}
			if test == "full queue" {
				c.queuedBytes = 0
				if err := c.initiateMultipathPath(1, ca, sa); err != nil {
					t.Fatal("failed attempt consumed path", err)
				}
			}
		})
	}
}

// Local initiation order is committed only after probe admission. It must not
// impose an ordering restriction on packets arriving at the server.
func TestMultipathInitiationOrder(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	p := c.multipath
	p.paths[1].application = nil
	for _, id := range []uint64{2, 3} {
		if _, err := p.issue(id); err != nil {
			t.Fatal(err)
		}
		cid := []byte{byte(id), 1, 2, 3, 4, 5, 6, 7}
		frame := wire.PathNewConnectionIDFrame{PathID: id, NewConnectionIDFrame: wire.NewConnectionIDFrame{ConnectionID: cid}}
		frame.StatelessResetToken[0] = byte(id)
		if _, err := p.receiveNew(frame); err != nil {
			t.Fatal(err)
		}
		for seq := range p.paths[id].local.active {
			p.markSent(id, seq)
		}
	}
	local := netip.MustParseAddrPort("192.0.2.1:443")
	remote := netip.MustParseAddrPort("192.0.2.2:444")
	c.queuedBytes = maxSendBytes
	if err := c.initiateMultipathPath(3, local, remote); !errors.Is(err, ErrSendBuffer) {
		t.Fatal(err)
	}
	if p.highestInitiated != 0 {
		t.Fatal("failed probe admission consumed ordering")
	}
	c.queuedBytes = 0
	if err := c.initiateMultipathPath(2, local, remote); err != nil {
		t.Fatal(err)
	}
	before := c.queuedBytes
	if err := c.initiateMultipathPath(1, local, remote); !errors.Is(err, errPathInitiation) {
		t.Fatal(err)
	}
	if p.paths[1].application != nil || c.queuedBytes != before || c.Err() != nil {
		t.Fatal("rejected lower ID changed connection")
	}
	if _, err := p.beginAbandon(2); err != nil {
		t.Fatal(err)
	}
	// Even when the higher path fails, lower skipped IDs stay unavailable.
	if err := c.initiateMultipathPath(1, local, remote); !errors.Is(err, errPathInitiation) {
		t.Fatal(err)
	}
	if err := c.initiateMultipathPath(3, local, remote); err != nil {
		t.Fatal(err)
	}
	if p.highestInitiated != 3 {
		t.Fatal(p.highestInitiated)
	}
}

func TestDisableActiveMigrationRestrictsHandshakeAddress(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		client, _ := sendingPathPair(t, version)
		local := netip.MustParseAddrPort("192.0.2.1:443")
		handshake := netip.MustParseAddrPort("192.0.2.2:444")
		other := netip.MustParseAddrPort("192.0.2.2:445")
		client.peerParams.DisableActiveMigration = true
		client.multipath.paths[1].application = nil
		if err := client.bindPreparedPath(0, local, handshake); err != nil {
			t.Fatal(err)
		}
		before := client.queuedBytes
		if err := client.initiateMultipathPath(1, local, handshake); !errors.Is(err, errPathInitiation) {
			t.Fatal("disabled handshake-address initiation accepted", err)
		}
		if client.queuedBytes != before || client.multipath.paths[1].application != nil || client.multipath.highestInitiated != 0 || client.Err() != nil {
			t.Fatal("policy rejection consumed state")
		}
		// Changing the current binding must not change the handshake-address policy.
		client.multipath.paths[0].application.network = &pathTuple{local, other}
		if err := client.initiateMultipathPath(1, local, handshake); !errors.Is(err, errPathInitiation) {
			t.Fatal("current binding replaced handshake identity")
		}
		if err := client.initiateMultipathPath(1, local, other); err != nil {
			t.Fatal("other peer address incorrectly prohibited", err)
		}
	}
}

func TestActiveMigrationAdvertisementRequiresCapability(t *testing.T) {
	_, template := testPair(t, wire.Version1)
	for _, capable := range []bool{false, true} {
		for _, explicitDisable := range []bool{false, true} {
			cfg := template.config
			cfg.Parameters.DisableActiveMigration = explicitDisable
			cfg.EnableMigration = true
			cfg.allowActiveMigration = capable
			c, err := NewConn(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if c.config.Parameters.DisableActiveMigration != (explicitDisable || !capable) {
				t.Fatal("migration advertisement differs from owner capability")
			}
			c.Close()
		}
	}
}
