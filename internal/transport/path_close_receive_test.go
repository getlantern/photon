package transport

import (
	"bytes"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestMultipathClosingReceiveAndCachedResponse(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := sendingPathPair(t, version)
		ca := netip.MustParseAddrPort("192.0.2.1:443")
		sa := netip.MustParseAddrPort("192.0.2.2:444")
		for _, c := range []*Conn{client, server} {
			if _, err := c.multipath.beginAbandon(0); err != nil {
				t.Fatal(err)
			}
			local, remote := ca, sa
			if c.config.Server {
				local, remote = sa, ca
			}
			if err := c.bindPreparedPath(1, local, remote); err != nil {
				t.Fatal(err)
			}
			c.multipath.paths[1].application.budget.Validate()
		}
		now := time.Unix(101, 0)
		if err := server.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
			t.Fatal(err)
		}
		_, _, ping, err := server.nextBoundMultipathPacket(now)
		if err != nil || ping == nil {
			t.Fatal(err)
		}
		if err := client.CloseWithError(42, "finished", now); err != nil {
			t.Fatal(err)
		}
		_, _, first, err := client.nextBoundMultipathPacket(now)
		if err != nil || first == nil {
			t.Fatal(err)
		}
		deadline := client.RecoveryDeadline()
		interval := client.closing.interval
		client.handleBoundMultipathPacket(ping, ca, sa, now)
		if _, _, b, err := client.nextBoundMultipathPacket(now); err != nil || b != nil {
			t.Fatal("close flood", err)
		}
		at := now.Add(interval)
		before := client.closing.responseCredit.credit
		bad := append([]byte(nil), ping...)
		bad[len(bad)-1] ^= 1
		client.handleBoundMultipathPacket(bad, ca, sa, at)
		client.handleBoundMultipathPacket(ping, ca, netip.MustParseAddrPort("192.0.2.3:444"), at)
		if client.closing.pending || client.closing.responseCredit.credit != before {
			t.Fatal("unauthenticated close trigger")
		}
		client.handleBoundMultipathPacket(ping, ca, sa, at)
		_, _, second, err := client.nextBoundMultipathPacket(at)
		if err != nil || !bytes.Equal(first, second) {
			t.Fatal("cached close response", err)
		}
		if client.multipath.paths[1].application.space.nextPN != 1 || client.RecoveryDeadline() != deadline {
			t.Fatal("response changed PN/deadline")
		}
		if err := server.CloseWithError(0, "peer done", at); err != nil {
			t.Fatal(err)
		}
		_, _, peerClose, err := server.nextBoundMultipathPacket(at)
		if err != nil || peerClose == nil {
			t.Fatal(err)
		}
		client.handleBoundMultipathPacket(peerClose, ca, sa, at)
		if !client.Draining() || client.Closing() || client.RecoveryDeadline() != deadline {
			t.Fatal("peer close did not preserve draining deadline")
		}
	}
}
