package transport

import (
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestMultipathIdleActivityOnSurvivingPath(t *testing.T) {
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
			c.config.Parameters.MaxIdleTimeout = 10000
			c.peerParams.MaxIdleTimeout = 10000
		}
		now := time.Unix(101, 0)
		client.idle.received(now)
		originalDeadline := client.IdleDeadline()
		if err := server.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
			t.Fatal(err)
		}
		id, _, packet, err := server.nextBoundMultipathPacket(now)
		if err != nil || id != 1 || packet == nil {
			t.Fatal(id, err)
		}
		damaged := append([]byte(nil), packet...)
		damaged[len(damaged)-1] ^= 1
		_ = client.handleBoundMultipathPacket(damaged, ca, sa, now.Add(time.Second))
		if client.IdleDeadline() != originalDeadline {
			t.Fatal("unauthenticated packet refreshed idle deadline")
		}
		received := now.Add(2 * time.Second)
		if err := client.handleBoundMultipathPacket(packet, ca, sa, received); err != nil {
			t.Fatal(err)
		}
		if client.IdleDeadline() != received.Add(10*time.Second) {
			t.Fatal("surviving path did not refresh connection idle deadline", client.IdleDeadline())
		}
		// Evaluating deadlines must not count as further activity.
		_ = client.RecoveryDeadline()
		if client.IdleDeadline() != received.Add(10*time.Second) {
			t.Fatal("deadline query refreshed idle activity")
		}
		expiry := client.IdleDeadline()
		if err := client.HandleTimeout(expiry); !errors.Is(err, ErrIdleTimeout) {
			t.Fatal("surviving-path idle expiry", err)
		}
		if packet, err := client.NextDatagram(expiry); len(packet) != 0 || !errors.Is(err, ErrIdleTimeout) {
			t.Fatal("idle expiry emitted traffic", err)
		}
		if !client.RecoveryDeadline().IsZero() {
			t.Fatal("idle expiry retained a timer")
		}
	}
}

func TestMultipathIdleFloorAfterPathDisposal(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := testPair(t, version)
		drivePair(t, client, server, false, false)
		client.config.Parameters.MaxIdleTimeout = 1
		client.peerParams.MaxIdleTimeout = 1
		client.peerParams.MaxAckDelay = 25
		now := time.Unix(101, 0)
		client.idle.received(now)
		base := client.IdleDeadline()
		attachSlowPath(t, client)
		extended := client.IdleDeadline()
		if !extended.After(base) {
			t.Fatal("slow path not included")
		}
		if _, err := client.multipath.beginAbandon(1); err != nil {
			t.Fatal(err)
		}
		if _, err := client.multipath.receiveAbandon(wire.PathAbandonFrame{PathID: 1}, now, time.Second); err != nil {
			t.Fatal(err)
		}
		if client.IdleDeadline() != extended {
			t.Fatal("retained path lost its PTO floor")
		}
		removed := client.multipath.collectAbandoned(now.Add(3 * time.Second))
		if len(removed) != 1 || removed[0].ID != 1 {
			t.Fatal("path not disposed", removed)
		}
		if client.IdleDeadline() != base {
			t.Fatal("disposed path still extends idle floor", client.IdleDeadline(), base)
		}
		if client.idle.last != now {
			t.Fatal("path disposal fabricated idle activity")
		}
	}
}
