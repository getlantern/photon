package transport

import (
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestMultipathUnroutableReset(t *testing.T) {
	for _, closing := range []bool{false, true} {
		c, _ := sendingPathPair(t, wire.Version1)
		now := time.Unix(101, 0)
		ca := netip.MustParseAddrPort("192.0.2.1:443")
		sa := netip.MustParseAddrPort("192.0.2.2:444")
		if err := c.bindPreparedPath(1, ca, sa); err != nil {
			t.Fatal(err)
		}
		state := c.multipath.paths[1]
		peer := state.peer.active[state.peer.current]
		peer.used = true
		state.peer.active[state.peer.current] = peer
		if closing {
			if _, err := c.multipath.beginAbandon(0); err != nil {
				t.Fatal(err)
			}
			if err := c.CloseWithError(0, "done", now); err != nil {
				t.Fatal(err)
			}
		}
		// A truncated long-header-looking prefix cannot be CID-routed.
		reset := append([]byte{0xc0, 0, 0, 0, 1}, peer.token[:]...)
		c.handleBoundMultipathPacket(reset, ca, netip.MustParseAddrPort("192.0.2.3:444"), now)
		if c.Draining() {
			t.Fatal("unknown tuple reset connection")
		}
		err := c.handleBoundMultipathPacket(reset, ca, sa, now)
		if !c.Draining() {
			t.Fatal("unroutable reset ignored", err)
		}
		if !closing && !errors.Is(err, ErrStatelessReset) {
			t.Fatal(err)
		}
	}
}

func TestMultipathAuthenticatedSuffixIsNotReset(t *testing.T) {
	for _, closing := range []bool{false, true} {
		c, peer := sendingPathPair(t, wire.Version2)
		now := time.Unix(101, 0)
		ca := netip.MustParseAddrPort("192.0.2.1:443")
		sa := netip.MustParseAddrPort("192.0.2.2:444")
		if err := c.bindPreparedPath(1, ca, sa); err != nil {
			t.Fatal(err)
		}
		peer.multipath.paths[1].application.budget.Validate()
		if err := peer.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
			t.Fatal(err)
		}
		raw, err := peer.nextPacketOnPath(1, now)
		if err != nil || raw == nil {
			t.Fatal(err)
		}
		state := c.multipath.paths[1]
		id := state.peer.active[state.peer.current]
		copy(id.token[:], raw[len(raw)-16:])
		id.used = true
		state.peer.active[state.peer.current] = id
		if closing {
			if _, err := c.multipath.beginAbandon(0); err != nil {
				t.Fatal(err)
			}
			if err := c.CloseWithError(0, "done", now); err != nil {
				t.Fatal(err)
			}
		}
		c.handleBoundMultipathPacket(raw, ca, sa, now)
		if c.Draining() {
			t.Fatal("authenticated suffix treated as reset")
		}
	}
}
