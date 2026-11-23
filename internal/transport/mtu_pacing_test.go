package transport

import (
	"github.com/getlantern/photon/internal/wire"
	"testing"
	"time"
)

func TestMTUProbeSpacingAndWakeup(t *testing.T) {
	for _, rtt := range []time.Duration{time.Millisecond, 2 * time.Second} {
		c, _ := sendingPathPair(t, wire.Version1)
		app := c.multipath.paths[1].application
		app.validation.state = PathValidated
		app.budget.Validate()
		app.sent.RTT.Smoothed = rtt
		if err := c.startPathMTUSearch(1, 1500); err != nil {
			t.Fatal(err)
		}
		now := time.Unix(101, 0)
		if b, err := c.nextPacketOnPath(1, now); err != nil || len(b) != 1350 {
			t.Fatal(err)
		}
		deadline := now.Add(max(time.Second, rtt))
		if app.mtuNextProbe != deadline {
			t.Fatal("wrong spacing deadline")
		}
		ack := wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}}}
		early := now.Add(time.Millisecond)
		if _, err := c.multipath.receiveACK(ack, early, 0, 0, false); err != nil {
			t.Fatal(err)
		}
		blocked := func(at time.Time) {
			t.Helper()
			pn := app.space.nextPN
			if b, err := c.nextPacketOnPath(1, at); err != nil || b != nil || app.space.nextPN != pn || app.mtu.pending {
				t.Fatal("early probe committed", err)
			}
			if app.pacingAt != deadline {
				t.Fatal("missing discovery wakeup")
			}
		}
		c.armPathRecovery(1, early)
		blocked(early)
		if got := c.multipathRecoveryDeadline(); got != deadline {
			t.Fatal("owner cannot wake discovery", got, deadline)
		}
		// This delay applies only to discovery, not normal queued output.
		if err := c.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
			t.Fatal(err)
		}
		if b, err := c.nextPacketOnPath(1, early); err != nil || b == nil || len(b) > 1200 || app.mtu.pending {
			t.Fatal("discovery spacing blocked ordinary output", err)
		}
		ack.Ranges = []wire.AckRange{{Smallest: 1, Largest: 1}}
		if _, err := c.multipath.receiveACK(ack, early.Add(time.Millisecond), 0, 0, false); err != nil {
			t.Fatal(err)
		}
		c.armPathRecovery(1, early.Add(time.Millisecond))
		blocked(deadline.Add(-time.Nanosecond))
		if err := c.expireMultipathRecovery(deadline); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3 && !app.mtu.pending; i++ {
			if _, err := c.nextPacketOnPath(1, deadline.Add(time.Duration(i)*time.Millisecond)); err != nil {
				t.Fatal(err)
			}
		}
		if !app.mtu.pending || app.mtu.candidate != 1425 {
			t.Fatal("discovery did not resume at deadline")
		}
	}
}
