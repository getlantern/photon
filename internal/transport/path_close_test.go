package transport

import (
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestMultipathCloseOnSurvivor(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := sendingPathPair(t, version)
		now := time.Unix(101, 0)
		ca := netip.MustParseAddrPort("192.0.2.1:443")
		sa := netip.MustParseAddrPort("192.0.2.2:444")
		if err := client.bindPreparedPath(1, ca, sa); err != nil {
			t.Fatal(err)
		}
		if err := server.bindPreparedPath(1, sa, ca); err != nil {
			t.Fatal(err)
		}
		if _, err := client.multipath.beginAbandon(0); err != nil {
			t.Fatal(err)
		}
		before := client.spaces[applicationSpace].nextPN
		app := client.multipath.paths[1].application
		app.budget.Validate()
		if err := client.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
			t.Fatal(err)
		}
		if err := client.CloseWithError(42, "done", now); err != nil {
			t.Fatal(err)
		}
		if app.space.nextPN != 1 || client.spaces[applicationSpace].nextPN != before || len(app.space.queue) != 0 {
			t.Fatal("close used wrong PN or retained output")
		}
		id, tuple, b, err := client.nextBoundMultipathPacket(now)
		if err != nil || id != 1 || tuple != (pathTuple{ca, sa}) || b == nil {
			t.Fatal(id, tuple, err)
		}
		if err := server.handleBoundMultipathPacket(b, sa, ca, now); !errors.Is(err, ErrPeerClosed) || !server.Draining() {
			t.Fatal("peer could not decrypt close", err)
		}
		until := client.RecoveryDeadline()
		if err := client.HandleTimeout(until); err != nil || !client.closed {
			t.Fatal("close expiry", err)
		}
	}
}

func TestMultipathCloseAmplificationBudget(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	now := time.Unix(101, 0)
	if _, err := c.multipath.beginAbandon(0); err != nil {
		t.Fatal(err)
	}
	app := c.multipath.paths[1].application
	if err := c.bindPreparedPath(1, netip.MustParseAddrPort("192.0.2.1:443"), netip.MustParseAddrPort("192.0.2.2:444")); err != nil {
		t.Fatal(err)
	}
	if err := c.CloseTransportError(0x10, 0, "no primary", now); err != nil {
		t.Fatal(err)
	}
	if _, _, b, err := c.nextBoundMultipathPacket(now); err != nil || b != nil {
		t.Fatal("close bypassed amplification", err)
	}
	if err := app.budget.Received(1200); err != nil {
		t.Fatal(err)
	}
	before := app.budget.credit
	_, _, b, err := c.nextBoundMultipathPacket(now)
	if err != nil || b == nil || app.budget.credit != before-uint64(len(b)) {
		t.Fatal("wrong close budget", err)
	}
	if app.space.nextPN != 1 {
		t.Fatal("blocked close resealed packet")
	}
}
