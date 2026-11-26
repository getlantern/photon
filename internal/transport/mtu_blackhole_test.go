package transport

import (
	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
	"testing"
	"time"
)

func TestMTUBlackholeRecoveryFeedback(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		c, _ := sendingPathPair(t, version)
		a := c.multipath.paths[1].application
		useNewReno(t, c)
		a.validation.state = PathValidated
		if err := c.startPathMTUSearch(1, 1500); err != nil {
			t.Fatal(err)
		}
		// Feed recovery metadata for larger data packets directly; this unit covers
		// the feedback path only. mtu_blackhole_packet_test.go sends real ones.
		a.mtu.confirmed = 1400
		now := time.Unix(101, 0)
		for n := uint64(0); n < 6; n++ {
			size := 1400
			if n >= 3 {
				size = 1200
			}
			if err := a.recordSent(recovery.Packet{Number: n, SentAt: now, Bytes: size, AckEliciting: true, InFlight: true}); err != nil {
				t.Fatal(err)
			}
		}
		window := a.congestion.Window()
		ack := wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 3, Largest: 5}}}}
		events, err := c.multipath.receiveACK(ack, now.Add(time.Millisecond), 0, 0, false)
		if err != nil || len(events.Lost) != 3 {
			t.Fatal("recovery feedback", events, err)
		}
		if a.mtu.confirmed != 1200 || a.mtu.upper != 1200 || a.mtuRaiseDeadline() != now.Add(time.Millisecond+10*time.Minute) {
			t.Fatal("size fallback or cooldown missing")
		}
		if a.sent.BytesInFlight != 0 || a.congestion.Window() >= window || a.space.nextPN != 6 {
			t.Fatal("fallback interfered with normal recovery")
		}
		if a.observeMTUBlackhole(events.Acked, events.Lost, now.Add(time.Second)) {
			t.Fatal("stale evidence retriggered")
		}
	}
}

func TestMTUBlackholeEvidenceFilters(t *testing.T) {
	now := time.Unix(101, 0)
	packet := func(n uint64, size int, probe bool) recovery.Packet {
		return recovery.Packet{Number: n, Bytes: size, AckEliciting: true, InFlight: true, MTUProbe: probe}
	}
	fresh := func() *pathApplication {
		a := newPathApplication(recovery.NewRenoAlgorithm)
		m, _ := newMTUSearch(1500)
		m.confirmed = 1400
		a.mtu = &m
		a.space.nextPN = 30
		return a
	}
	large := []recovery.Packet{packet(0, 1400, false), packet(1, 1400, false), packet(2, 1400, false)}
	a := fresh()
	if a.observeMTUBlackhole(nil, large, now) {
		t.Fatal("loss without smaller success diagnosed MTU")
	}
	if a.observeMTUBlackhole([]recovery.Packet{packet(3, 1400, false)}, nil, now) || a.mtuBlackhole.losses != 0 {
		t.Fatal("newer large success did not invalidate failures")
	}
	if a.observeMTUBlackhole([]recovery.Packet{packet(4, 1200, false)}, large, now) {
		t.Fatal("old losses defeated newer large success")
	}
	a = fresh()
	a.observeMTUBlackhole(nil, large, now)
	if a.observeMTUBlackhole([]recovery.Packet{packet(4, 1500, true)}, nil, now) || a.mtuBlackhole.losses != 0 {
		t.Fatal("successful larger probe did not clear older failures")
	}
	a = fresh()
	probes := []recovery.Packet{packet(0, 1450, true), packet(1, 1450, true), packet(2, 1450, true)}
	if a.observeMTUBlackhole([]recovery.Packet{packet(3, 1200, false)}, probes, now) || a.mtuBlackhole.losses != 0 {
		t.Fatal("probe loss entered data failure heuristic")
	}
	a = fresh()
	if a.observeMTUBlackhole([]recovery.Packet{packet(0, 1200, false)}, []recovery.Packet{packet(1, 1400, false), packet(2, 1400, false), packet(3, 1400, false)}, now) {
		t.Fatal("older small ACK confirmed current reachability")
	}
	if !a.observeMTUBlackhole([]recovery.Packet{packet(4, 1200, false)}, nil, now) {
		t.Fatal("later small ACK failed to trigger conservative fallback")
	}
	a = fresh()
	a.resetMTU()
	a.mtu.confirmed = 1400
	if a.observeMTUBlackhole([]recovery.Packet{packet(4, 1200, false)}, large, now) {
		t.Fatal("old-tuple evidence survived migration")
	}
}
