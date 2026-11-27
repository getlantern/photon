package recovery

import (
	"math"
	"testing"
	"time"
)

// deliverTime simulates b on link and reports when it had delivered n bytes.
func deliverTime(t *testing.T, b *BBR, link *simLink, n uint64, d time.Duration) time.Duration {
	t.Helper()
	var at time.Duration
	simulate(t, b, link, d, func(now time.Time) {
		if at == 0 && b.delivered >= n {
			at = now.Sub(simStart)
		}
	})
	if at == 0 {
		t.Fatalf("delivered %d of %d bytes in %v", b.delivered, n, d)
	}
	return at
}

// On a 100 Mbit/s, 100 ms path whose state was saved, a resumed connection
// jumps after its first round and delivers 20 MB sooner than one starting
// from the initial window.
func TestCarefulResumeIsFaster(t *testing.T) {
	const n = 20 << 20
	saved := ResumeState{Bandwidth: 100e6 / 8, MinRTT: 100 * time.Millisecond}
	fresh := deliverTime(t, newBBRFlow(3)().(*BBR), bufferedLink(100e6, 100*time.Millisecond, 1), n, 10*time.Second)
	b := newBBRFlow(3)().(*BBR)
	b.Resume(saved, 0)
	resumed := deliverTime(t, b, bufferedLink(100e6, 100*time.Millisecond, 1), n, 10*time.Second)
	t.Logf("20 MB: %v fresh, %v resumed", fresh, resumed)
	if resumed > fresh-200*time.Millisecond {
		t.Fatalf("resumed %v, fresh %v: want at least two RTTs sooner", resumed, fresh)
	}
	if b.ResumeFailed() {
		t.Fatal("a jump to half the saved window failed on the same path")
	}
}

// When the link has slowed since the state was saved, the jump meets
// congestion and Safe Retreat ends it: the resume is marked failed, the
// window falls to half of what was delivered, and the connection loses no
// more than one starting fresh would lose overshooting in Startup, give or
// take the jump's own excess.
func TestCarefulResumeRetreatsOnSlowerLink(t *testing.T) {
	saved := ResumeState{Bandwidth: 100e6 / 8, MinRTT: 100 * time.Millisecond}
	b := newBBRFlow(3)().(*BBR)
	b.Resume(saved, 0)
	var retreated bool
	var cwndAtRetreat uint64
	r := simulate(t, b, bufferedLink(20e6, 100*time.Millisecond, 1), 10*time.Second, func(time.Time) {
		if b.cr.phase == crRetreat && !retreated {
			retreated, cwndAtRetreat = true, b.cwnd
		}
	})
	fresh := simulate(t, newBBRFlow(3)().(*BBR), bufferedLink(20e6, 100*time.Millisecond, 1), 10*time.Second, nil)
	t.Logf("retreated %v, window then %d; drops %d of %d resumed, %d of %d fresh; goodput %.1f vs %.1f Mbit/s",
		retreated, cwndAtRetreat, r.dropped, r.sent, fresh.dropped, fresh.sent, goodput(r, 10*time.Second)/1e6, goodput(fresh, 10*time.Second)/1e6)
	if !retreated || !b.ResumeFailed() {
		t.Fatalf("retreated %v, failed %v: want a Safe Retreat", retreated, b.ResumeFailed())
	}
	// The jump was 625 KB; the path held 250 KB plus a 250 KB queue.
	if cwndAtRetreat > 600_000 {
		t.Fatalf("window %d at retreat, want at most half of what was delivered", cwndAtRetreat)
	}
	if r.dropped > 2*fresh.dropped+500 {
		t.Fatalf("%d drops resumed, %d fresh", r.dropped, fresh.dropped)
	}
	if goodput(r, 10*time.Second) < 0.9*goodput(fresh, 10*time.Second) {
		t.Fatal("resumed goodput well below a fresh start's")
	}
}

// An RTT unlike the saved one means the path has changed: no jump.
func TestCarefulResumeSkipsChangedPath(t *testing.T) {
	for _, savedRTT := range []time.Duration{250 * time.Millisecond, 5 * time.Millisecond} {
		b := newBBRFlow(3)().(*BBR)
		b.Resume(ResumeState{Bandwidth: 100e6 / 8, MinRTT: savedRTT}, 0)
		jumped := false
		simulate(t, b, bufferedLink(100e6, 100*time.Millisecond, 1), 2*time.Second, func(time.Time) {
			jumped = jumped || b.cr.phase == crUnvalidated
		})
		if jumped || b.cr.phase != crOff || b.ResumeFailed() {
			t.Fatalf("saved RTT %v on a 100 ms path: jumped %v, phase %d, failed %v", savedRTT, jumped, b.cr.phase, b.ResumeFailed())
		}
	}
}

// A resumed newcomer takes little more from a flow already using the path
// than a fresh one does: its jump, half the saved window, fits the queue
// beside the incumbent's flight, and it goes on as a fresh one would.
func TestCarefulResumeBesideIncumbent(t *testing.T) {
	share := func(resume bool) (float64, bool) {
		inc := newBBRFlow(3)().(*BBR)
		b := newBBRFlow(7)().(*BBR)
		if resume {
			b.Resume(ResumeState{Bandwidth: 100e6 / 8, MinRTT: 50 * time.Millisecond}, 0)
		}
		flows := []*simFlow{{c: inc, snapshotAt: simStart.Add(5 * time.Second)}, {c: b, start: simStart.Add(5 * time.Second), snapshotAt: simStart.Add(5 * time.Second)}}
		simulateFlows(t, bufferedLink(100e6, 50*time.Millisecond, 1), 10*time.Second, flows...)
		got := func(f *simFlow) float64 { return float64(f.res.delivered - f.snapshot.delivered) }
		return got(flows[0]) / (got(flows[0]) + got(flows[1])), b.ResumeFailed()
	}
	fresh, _ := share(false)
	resumed, failed := share(true)
	t.Logf("incumbent's share of the 5 s after a newcomer starts: %.0f%% beside a fresh one, %.0f%% beside a resumed one (failed %v)", 100*fresh, 100*resumed, failed)
	if resumed < fresh-0.1 {
		t.Fatalf("incumbent kept %.0f%% beside a resumed newcomer, %.0f%% beside a fresh one", 100*resumed, 100*fresh)
	}
}

// Snapshot saves only a measured path: after Startup, not in ProbeRTT, and
// at least four initial windows' worth.
func TestBBRSnapshot(t *testing.T) {
	b := newBBRFlow(3)().(*BBR)
	if _, ok := b.Snapshot(); ok {
		t.Fatal("snapshot of a fresh controller")
	}
	simulate(t, b, newLink(50e6, 50*time.Millisecond, 0), 3*time.Second, nil)
	s, ok := b.Snapshot()
	if !ok || s.Bandwidth < 0.9*50e6/8 || s.MinRTT < 50*time.Millisecond || s.MinRTT > 55*time.Millisecond {
		t.Fatalf("snapshot %+v, %v after 3 s on a 50 Mbit/s, 50 ms path", s, ok)
	}
	b.state = bbrProbeRTT
	if _, ok := b.Snapshot(); ok {
		t.Fatal("snapshot in ProbeRTT")
	}
	small := newBBRFlow(3)().(*BBR)
	simulate(t, small, newLink(2e6, 20*time.Millisecond, 0), 3*time.Second, nil)
	if s, ok := small.Snapshot(); ok {
		t.Fatalf("snapshot %+v of a path under four initial windows", s)
	}
}

// Reconnaissance waits while the sender is rate limited, the jump is half
// the saved window or maxJump, paced over the current RTT, and it ends
// after two rounds.
func TestCarefulResumeJump(t *testing.T) {
	saved := ResumeState{Bandwidth: 10e6, MinRTT: 100 * time.Millisecond} // a 1 MB window
	for _, tc := range []struct {
		maxJump, want uint64
	}{{0, 500_000}, {300_000, 300_000}} {
		b := NewBBR(1200)
		b.Resume(saved, tc.maxJump)
		// After the first round: the RTT measured, and the window and
		// pacing rate of a Startup that delivered the initial window.
		b.delivered, b.minRTT, b.pacingRate = b.initialWindow, 90*time.Millisecond, 2e6
		b.crOnACK(RateSample{AppLimited: true})
		if b.cr.phase != crReconnaissance {
			t.Fatal("left Reconnaissance while rate limited")
		}
		b.crOnACK(RateSample{})
		if b.cr.phase != crUnvalidated || b.cr.jump != tc.want {
			t.Fatalf("max jump %d: phase %d, jump %d, want %d", tc.maxJump, b.cr.phase, b.cr.jump, tc.want)
		}
		if rate := float64(tc.want) / 0.09; math.Abs(b.cr.rate-rate) > 1 {
			t.Fatalf("pacing %.0f, want %.0f: the jump over the current RTT", b.cr.rate, rate)
		}
		b.crBound()
		if b.cwnd != tc.want || b.pacingRate != b.cr.rate {
			t.Fatalf("window %d, pacing %.0f: want the jump's", b.cwnd, b.pacingRate)
		}
		b.roundCount += 2
		b.crOnACK(RateSample{})
		if b.cr.phase != crOff || b.ResumeFailed() {
			t.Fatal("jump not over after two rounds")
		}
	}
}

// Congestion in Reconnaissance ends Careful Resume without failing it;
// after the jump it is Safe Retreat, and persistent congestion keeps the
// failure that its reset would otherwise forget.
func TestCarefulResumeCongestion(t *testing.T) {
	saved := ResumeState{Bandwidth: 10e6, MinRTT: 100 * time.Millisecond}
	b := NewBBR(1200)
	b.Resume(saved, 0)
	b.noteLoss()
	if b.cr.phase != crOff || b.ResumeFailed() {
		t.Fatal("loss in Reconnaissance")
	}
	b = NewBBR(1200)
	b.Resume(saved, 0)
	b.OnPersistentCongestion()
	if b.ResumeFailed() {
		t.Fatal("persistent congestion in Reconnaissance, before any jump, failed it")
	}
	b = NewBBR(1200)
	b.Resume(saved, 0)
	b.delivered, b.minRTT = b.initialWindow, 100*time.Millisecond
	b.crOnACK(RateSample{})
	b.crBound()
	// The loss enters recovery, which saves the jump's window to restore.
	b.OnLoss(LossEvent{Lost: []Packet{{Bytes: 1200, InFlight: true}}, TotalLost: 1200, Flight: 400_000, TotalDelivered: b.delivered, Now: time.Unix(1, 0)})
	if b.cr.phase != crRetreat || !b.ResumeFailed() || b.state != bbrDrain || b.cwnd > 200_000 || b.priorCwnd > b.cr.retreatCwnd {
		t.Fatalf("loss after the jump: phase %d, failed %v, state %d, window %d", b.cr.phase, b.ResumeFailed(), b.state, b.cwnd)
	}
	// The retreat's window grows no more, and Drain's rates don't count.
	b.cwnd = 300_000
	b.crBound()
	if b.cwnd != b.cr.retreatCwnd {
		t.Fatalf("window %d in Safe Retreat, want at most %d", b.cwnd, b.cr.retreatCwnd)
	}
	maxBw := b.maxBw
	b.updateMaxBw(RateSample{Valid: true, Rate: uint64(maxBw) + 1e6})
	if b.maxBw != maxBw {
		t.Fatal("a rate measured in the retreat's Drain counted")
	}
	b.OnPersistentCongestion()
	if !b.ResumeFailed() {
		t.Fatal("persistent congestion forgot the failure")
	}
}
