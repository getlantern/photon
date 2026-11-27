package recovery

import (
	"math/rand/v2"
	"testing"
	"time"
)

// On a path that loses packets at random, without congestion, BBR keeps most
// of the link: the loss it learns as the path's baseline doesn't count as
// congestion. Under draft-07's responses it kept 9-75% at these rates.
func TestBBRHoldsLinkUnderRandomLoss(t *testing.T) {
	const d = 20 * time.Second
	for _, rtt := range []time.Duration{50 * time.Millisecond, 150 * time.Millisecond} {
		for _, loss := range []float64{0.02, 0.05} {
			b := NewBBR(1200)
			b.rng = rand.New(rand.NewPCG(3, 4))
			g := goodput(simulate(t, b, newLink(50e6, rtt, loss), d, nil), d)
			t.Logf("%v, %.0f%% loss: %.1f Mbit/s, baseline %.2f%%", rtt, 100*loss, g/1e6, 100*b.baseline.rate)
			if g < 0.85*50e6 {
				t.Fatalf("%v, %.0f%% loss: %.1f Mbit/s, want at least 42.5 of 50", rtt, 100*loss, g/1e6)
			}
			if b.baseline.rate < 0.5*loss || b.baseline.rate > 1.5*loss {
				t.Fatalf("%v, %.0f%% loss: baseline %.2f%%", rtt, 100*loss, 100*b.baseline.rate)
			}
		}
	}
}

// Where loss means congestion, the baseline stays at about zero throughout
// and BBR responds as draft-07 has it: a clean path, a shallow buffer, and a
// queue shared with two NewReno flows, whose loss shows with a queue and is
// kept out. A policer is the exception it tolerates: cruising at the policed
// rate, BBR overruns the bucket now and then with no queue to show it, and
// learns a baseline of up to a few percent at times.
func TestLossBaselineStaysZeroUnderCongestion(t *testing.T) {
	policed := bufferedLink(100e6, 50*time.Millisecond, 1)
	policed.policer = &simPolicer{rate: 10e6 / 8, burst: 64 * 1024}
	for _, tc := range []struct {
		name   string
		link   *simLink
		renos  int
		maxTop float64
	}{
		{"clean", bufferedLink(100e6, 50*time.Millisecond, 1), 0, 0.002},
		{"policer", policed, 0, 0.05},
		{"shallow buffer", bufferedLink(100e6, 50*time.Millisecond, 0.1), 0, 0.002},
		{"with NewReno", bufferedLink(10e6, 20*time.Millisecond, 1), 2, 0.002},
	} {
		b := newBBRFlow(3)().(*BBR)
		top := 0.0
		flows := []*simFlow{{c: b, observe: func(time.Time) { top = max(top, b.baseline.rate) }}}
		for i := range tc.renos {
			flows = append(flows, &simFlow{c: newRenoFlow(), start: simStart.Add(time.Duration(2+i) * time.Second)})
		}
		simulateFlows(t, tc.link, 30*time.Second, flows...)
		t.Logf("%s: baseline at most %.3f%%, %.3f%% at the end", tc.name, 100*top, 100*b.baseline.rate)
		if top > tc.maxTop {
			t.Fatalf("%s: baseline reached %.3f%%, want at most %.1f%%", tc.name, 100*top, 100*tc.maxTop)
		}
	}
}

// The baseline averages quiet rounds' loss, ignores rounds BBR began while
// probing, the round after one, whose loss is the probe's, and rounds with
// a queue, and caps what it discounts.
func TestLossBaselineRounds(t *testing.T) {
	noQueue := func(time.Duration) bool { return false }
	queue := func(time.Duration) bool { return true }
	var l lossBaseline
	l.endRound(0, true, noQueue)
	l.endRound(0, true, noQueue)
	// Quiet rounds after quiet ones, each losing 50 of 1000 bytes: the
	// baseline waits for bbrMinBaselineRounds of them.
	for i := range bbrMinBaselineRounds {
		if l.rate != 0 {
			t.Fatal("baseline after", i, "rounds", l.rate)
		}
		l.noteLost(50)
		l.noteRTT(time.Millisecond)
		quiet := i < bbrMinBaselineRounds-1
		l.endRound(l.roundDelivered+950, quiet, noQueue)
	}
	if l.rate != 0.05 || l.lastRoundRate != 0.05 || l.lastRoundSent != 1000 {
		t.Fatal("quiet rounds", l.rate, l.lastRoundRate, l.lastRoundSent)
	}
	// A probing round counts toward the last round's rate, not the baseline.
	l.noteLost(500)
	l.endRound(l.roundDelivered+500, true, noQueue)
	if l.rate != 0.05 || l.lastRoundRate != 0.5 {
		t.Fatal("probing round", l.rate, l.lastRoundRate)
	}
	// So does the quiet round after it, which sees the probe's loss.
	l.noteLost(500)
	l.endRound(l.roundDelivered+500, true, noQueue)
	if l.rate != 0.05 {
		t.Fatal("round after the probe", l.rate)
	}
	// A quiet round with a queue is kept out too.
	l.noteLost(500)
	l.noteRTT(time.Millisecond)
	l.endRound(l.roundDelivered+500, true, queue)
	if l.rate != 0.05 {
		t.Fatal("queued round", l.rate)
	}
	// A lossless quiet round lowers it; heavy loss is capped.
	l.endRound(l.roundDelivered+1000, true, noQueue)
	if l.rate != 200.0/5000 {
		t.Fatal("lossless round", l.rate)
	}
	for range bbrBaselineRounds {
		l.noteLost(900)
		l.endRound(l.roundDelivered+100, true, noQueue)
	}
	if l.rate != bbrMaxLossBaseline {
		t.Fatal("cap", l.rate)
	} // Rounds that all show a queue keep the baseline, until there have been
	// bbrStaleBaselineRounds of them.
	for range bbrStaleBaselineRounds - 1 {
		l.noteRTT(time.Millisecond)
		l.endRound(l.roundDelivered+1000, true, queue)
	}
	if l.rate != bbrMaxLossBaseline {
		t.Fatal("queued rounds", l.rate)
	}
	l.noteRTT(time.Millisecond)
	l.endRound(l.roundDelivered+1000, true, queue)
	if l.rate != 0 || l.count != 0 {
		t.Fatal("stale", l.rate, l.count)
	}
}

// Without a baseline any loss in a round is congestion, as draft-07 has it.
// With one, a round's loss must exceed it by the threshold and by sampling
// noise: three standard deviations, wider for a short round, but at most
// bbrMaxLossMargin.
func TestRoundCongested(t *testing.T) {
	b := NewBBR(1200)
	b.baseline.lastRoundRate, b.baseline.lastRoundSent = 0.001, 1200*1000
	if !b.roundCongested() {
		t.Fatal("no baseline: any loss is congestion")
	}
	b.baseline.rate = 0.05
	// 1000 packets: margin 3*sqrt(0.05*0.95/1000) = 2.1%, so above 9.1%.
	for _, tc := range []struct {
		rate    float64
		packets uint64
		want    bool
	}{{0.08, 1000, false}, {0.095, 1000, true}, {0.095, 100, false}, {0.15, 100, true}, {0.13, 10, true}} {
		b.baseline.lastRoundRate, b.baseline.lastRoundSent = tc.rate, tc.packets*1200
		if got := b.roundCongested(); got != tc.want {
			t.Fatalf("round loss %.1f%% of %d packets: congested %v, want %v", 100*tc.rate, tc.packets, got, tc.want)
		}
	}
}

// The baseline discounts lost bytes, never CE marks: those always mean
// congestion, for the in-flight limit and for the short-term bounds.
func TestBaselineDiscountsLossNotCE(t *testing.T) {
	b := NewBBR(1200)
	b.baseline.rate = 0.05
	for _, tc := range []struct {
		lost, ce uint64
		want     bool
	}{{60, 0, false}, {80, 0, true}, {0, 30, true}, {40, 15, false}, {40, 25, true}} {
		if got := b.isInflightTooHigh(tc.lost, tc.ce, 1000); got != tc.want {
			t.Fatalf("%d lost and %d marked of 1000 at a 5%% baseline: too high %v, want %v", tc.lost, tc.ce, got, tc.want)
		}
	}
	b.baseline.lastRoundRate, b.baseline.lastRoundSent = 0, 1200*1000
	if b.roundCongested() {
		t.Fatal("no loss or marks")
	}
	b.ceInRound = true
	if !b.roundCongested() {
		t.Fatal("CE marks with a baseline")
	}
}

// A queue shows as an RTT above the minimum by an eighth, between 4 and
// 16 ms; before any RTT, there is no telling, so it counts as one.
func TestBBRQueueing(t *testing.T) {
	b := NewBBR(1200)
	if !b.queueing(50 * time.Millisecond) {
		t.Fatal("no minimum RTT yet")
	}
	b.minRTT = 50 * time.Millisecond
	for _, tc := range []struct {
		rtt  time.Duration
		want bool
	}{{55 * time.Millisecond, false}, {57 * time.Millisecond, true}, {0, true}} {
		if got := b.queueing(tc.rtt); got != tc.want {
			t.Fatalf("RTT %v over 50ms: queueing %v, want %v", tc.rtt, got, tc.want)
		}
	}
	b.minRTT = 400 * time.Millisecond
	if b.queueing(415*time.Millisecond) || !b.queueing(416*time.Millisecond) {
		t.Fatal("16 ms cap")
	}
}

// On a buffer too shallow to show a queue, Startup still ends on loss: an
// overflow loses more than any baseline would discount. Under a queue
// requirement alone it ran on, dropping 6.2%, 14.1% and 4.7% of its first
// 3 s of packets on these paths.
func TestBBRStartupExitsOnShallowBufferLoss(t *testing.T) {
	for _, tc := range []struct {
		bps      float64
		rtt      time.Duration
		bdps     float64
		maxDrops float64
	}{
		{100e6, 50 * time.Millisecond, 0.1, 0.045},
		{100e6, 100 * time.Millisecond, 0.05, 0.1},
		{50e6, 30 * time.Millisecond, 0.1, 0.03},
	} {
		r := simulate(t, newBBRFlow(3)(), bufferedLink(tc.bps, tc.rtt, tc.bdps), 3*time.Second, nil)
		drops := float64(r.dropped) / float64(max(r.sent, 1))
		t.Logf("%.0f Mbit/s, %v, %.2f BDP: %.1f%% of packets dropped", tc.bps/1e6, tc.rtt, tc.bdps, 100*drops)
		if drops > tc.maxDrops {
			t.Fatalf("%.0f Mbit/s, %v, %.2f BDP: %.1f%% dropped, want at most %.1f%%", tc.bps/1e6, tc.rtt, tc.bdps, 100*drops, 100*tc.maxDrops)
		}
	}
}
