package transport

import (
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestMultipathAddressBinding(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	local := netip.MustParseAddrPort("192.0.2.1:443")
	remote := netip.MustParseAddrPort("192.0.2.2:444")
	for _, bad := range []netip.AddrPort{{}, netip.MustParseAddrPort("0.0.0.0:443"), netip.MustParseAddrPort("[::ffff:0.0.0.0]:443"), netip.MustParseAddrPort("224.0.0.1:443"), netip.MustParseAddrPort("192.0.2.1:0")} {
		if err := c.bindPreparedPath(1, bad, remote); !errors.Is(err, errPathBinding) {
			t.Fatal("invalid local", bad, err)
		}
	}
	if err := c.bindPreparedPath(1, local, remote); err != nil {
		t.Fatal(err)
	}
	if err := c.bindPreparedPath(1, netip.MustParseAddrPort("[::ffff:192.0.2.1]:443"), remote); err != nil {
		t.Fatal("canonical binding", err)
	}
	if err := c.bindPreparedPath(1, local, netip.MustParseAddrPort("192.0.2.2:445")); !errors.Is(err, errPathBinding) {
		t.Fatal("silent rebinding", err)
	}
	if got := c.multipath.paths[1].application.network.remote; got != remote {
		t.Fatal(got)
	}
	if c.multipath.paths[1].application.budget.Validated() {
		t.Fatal("binding granted validation")
	}
}

func TestMultipathBoundPacketAddressAndCredit(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := sendingPathPair(t, version)
		now := time.Unix(101, 0)
		ca := netip.MustParseAddrPort("192.0.2.1:443")
		sa := netip.MustParseAddrPort("192.0.2.2:444")
		for _, id := range []uint64{0, 1} {
			if err := client.bindPreparedPath(id, ca, sa); err != nil {
				t.Fatal(err)
			}
			if err := server.bindPreparedPath(id, sa, ca); err != nil {
				t.Fatal(err)
			}
		}
		client.multipath.paths[1].application.budget.Validate()
		if err := client.validateMultipathPath(1); err != nil {
			t.Fatal(err)
		}
		id, tuple, raw, err := client.nextBoundMultipathPacket(now)
		if err != nil || id != 1 || tuple != (pathTuple{ca, sa}) || len(raw) != 1200 {
			t.Fatal(id, tuple, len(raw), err)
		}
		app := server.multipath.paths[1].application
		authenticated := server.authenticatedPackets
		idle := server.idle
		wrong := netip.MustParseAddrPort("192.0.2.3:444")
		for _, tuple := range []pathTuple{{sa, wrong}, {wrong, ca}} {
			if err := server.handleBoundMultipathPacket(raw, tuple.local, tuple.remote, now); err != nil {
				t.Fatal(err)
			}
		}
		if app.budget.credit != 0 || app.space.received.Expected() != 0 || app.space.ackPending || server.authenticatedPackets != authenticated || server.idle != idle {
			t.Fatal("wrong tuple changed state")
		}
		corrupt := append([]byte(nil), raw...)
		corrupt[len(corrupt)-1] ^= 1
		if err := server.handleBoundMultipathPacket(corrupt, sa, ca, now); err != nil {
			t.Fatal(err)
		}
		if app.budget.credit != 0 || server.idle != idle {
			t.Fatal("unauthenticated credit/activity")
		}
		if err := server.handleBoundMultipathPacket(raw, sa, ca, now); err != nil {
			t.Fatal(err)
		}
		if app.budget.credit != 3600 || len(app.space.queue) != 1 {
			t.Fatal("bound packet credit/response", app.budget.credit, len(app.space.queue))
		}
		if err := server.handleBoundMultipathPacket(raw, sa, ca, now); err != nil {
			t.Fatal(err)
		}
		if app.budget.credit != 7200 || len(app.space.queue) != 1 {
			t.Fatal("replay dispatched frames")
		}
	}
}

func TestMultipathUnboundSendDoesNotCommit(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	now := time.Unix(101, 0)
	app := c.multipath.paths[1].application
	app.budget.Validate()
	if err := c.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
		t.Fatal(err)
	}
	before := c.sealed[applicationSpace]
	if _, _, b, err := c.nextBoundMultipathPacket(now); !errors.Is(err, errPathBinding) || b != nil {
		t.Fatal(err)
	}
	if app.space.nextPN != 0 || c.sealed[applicationSpace] != before || len(app.space.queue) != 1 {
		t.Fatal("unbound send committed")
	}
}
