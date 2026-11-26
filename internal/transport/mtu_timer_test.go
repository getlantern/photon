package transport

import (
	"github.com/getlantern/photon/internal/wire"
	"testing"
	"time"
)

func TestMTUTimeoutPreservesRecoveryAndLateACK(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		c, _ := sendingPathPair(t, version)
		// A probe timeout mustn't cut the window, which only NewReno would
		// otherwise do.
		useNewReno(t, c)
		a := c.multipath.paths[1].application
		a.validation.state = PathValidated
		a.budget.Validate()
		if err := c.startPathMTUSearch(1, 1500); err != nil {
			t.Fatal(err)
		}
		now := time.Unix(101, 0)
		if b, err := c.nextPacketOnPath(1, now); err != nil || len(b) != 1350 {
			t.Fatal(err)
		}
		deadline := a.mtuProbeDeadline()
		if deadline.Before(now.Add(16 * time.Second)) {
			t.Fatal("probe wait too short")
		}
		if at := c.multipathRecoveryDeadline(); at.IsZero() || at.After(deadline) {
			t.Fatal("owner misses discovery timeout")
		}
		flight, window, pn := a.sent.BytesInFlight, a.congestion.Window(), a.space.nextPN
		if a.expireMTUProbe(deadline.Add(-time.Nanosecond)) {
			t.Fatal("early timeout")
		}
		if err := c.expireMultipathRecovery(deadline); err != nil {
			t.Fatal(err)
		}
		if a.mtu.pending || a.mtu.confirmed != 1200 || a.mtu.attempts != 1 || a.sent.BytesInFlight != flight || a.congestion.Window() != window || a.space.nextPN != pn {
			t.Fatal("discovery timeout changed recovery or failed to release search")
		}
		if a.expireMTUProbe(deadline) || !a.mtuProbeDeadline().IsZero() {
			t.Fatal("timeout repeated")
		}
		// Ordinary PTO output may go first; the next dedicated probe gets a fresh
		// packet number and restarts its own ACK wait, without dropping old flight.
		now = deadline
		for i := 0; i < 4 && !a.mtu.pending; i++ {
			if _, err := c.nextPacketOnPath(1, now); err != nil {
				t.Fatal(err)
			}
			now = now.Add(time.Second)
		}
		if !a.mtu.pending || a.mtu.attempts != 2 || a.mtu.lastNumber == 0 || a.sent.BytesInFlight < 2700 || !a.mtuProbeDeadline().After(deadline) {
			t.Fatal("missing fresh retry")
		}
		ack := wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}}}
		if _, err := c.multipath.receiveACK(ack, now, 0, 0, false); err != nil {
			t.Fatal(err)
		}
		if a.mtu.confirmed != 1350 || a.mtu.pending || !a.mtuProbeDeadline().IsZero() || a.expireMTUProbe(now.Add(time.Hour)) {
			t.Fatal("late ACK did not cancel candidate wait")
		}
	}
}

func TestMTUTimeoutUsesPathPTO(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	a := c.multipath.paths[1].application
	a.validation.state = PathValidated
	a.budget.Validate()
	a.sent.RTT.Smoothed = 10 * time.Second
	a.sent.RTT.Variation = 2 * time.Second
	if err := c.startPathMTUSearch(1, 1500); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(101, 0)
	if b, err := c.nextPacketOnPath(1, now); err != nil || b == nil {
		t.Fatal(err)
	}
	want := now.Add(triple(a.sent.RTT.PTO(true, true, time.Duration(c.peerParams.MaxAckDelay)*time.Millisecond, 0)))
	if a.mtuProbeDeadline() != want || !want.After(now.Add(16*time.Second)) {
		t.Fatal("slow path wait not respected")
	}
	a.mtu.reset()
	if !a.mtuProbeDeadline().IsZero() || a.expireMTUProbe(want) {
		t.Fatal("old timer survived migration reset")
	}
}

func TestMTUTimeoutAttemptBound(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version2)
	a := c.multipath.paths[1].application
	a.validation.state = PathValidated
	a.budget.Validate()
	if err := c.startPathMTUSearch(1, 1500); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(101, 0)
	for attempt := 1; attempt <= mtuProbeAttempts; attempt++ {
		if b, err := c.nextPacketOnPath(1, now); err != nil || len(b) != 1350 {
			t.Fatal("retry", attempt, len(b), err)
		}
		if a.mtu.attempts != attempt {
			t.Fatal("attempt not committed")
		}
		now = a.mtuProbeDeadline()
		if !a.expireMTUProbe(now) {
			t.Fatal("attempt not expired")
		}
	}
	if a.mtu.confirmed != 1200 || a.mtu.upper != 1349 || a.mtu.attempts != 0 || a.sent.BytesInFlight != 3*1350 {
		t.Fatal("bounded timeout changed confirmed size or recovery flight")
	}
	if a.mtu.acknowledged(0) {
		t.Fatal("abandoned-size ACK resurrected candidate")
	}
	if b, err := c.nextPacketOnPath(1, now); err != nil || len(b) != 1275 || a.mtu.attempts != 1 {
		t.Fatal("search did not narrow", len(b), err)
	}
}
