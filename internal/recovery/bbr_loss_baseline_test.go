package recovery

import (
	"math"
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

// CE marks are congestion whatever the loss: cuts they made to the
// short-term bounds stay when the first baseline arrives, and others go.
func TestFirstBaselineKeepsCECuts(t *testing.T) {
	for _, ce := range []bool{false, true} {
		b := NewBBR(1200)
		b.state, b.maxBw, b.cwnd = bbrProbeBWCruise, 1e6, 100000
		b.bwLatest, b.inflightLatest = 5e5, 50000
		b.lossInRound, b.ceInRound = true, ce
		b.adaptLowerBoundsFromCongestion()
		if math.IsInf(b.bwShortterm, 1) {
			t.Fatal("no cut")
		}
		b.lossInRound, b.ceInRound = false, false
		b.dropPreBaselineCuts()
		if kept := !math.IsInf(b.bwShortterm, 1); kept != ce {
			t.Fatalf("CE cut %v: kept %v", ce, kept)
		}
	}
}

// The cuts to the short-term bounds made before there was a baseline
// answered random loss, so the first baseline drops them: a short transfer
// would otherwise cruise below the link until its next probe. On a 50 ms
// path at 1% loss they had BBR pacing at 96% of the link.
func TestBBRFirstBaselineDropsShortTermCuts(t *testing.T) {
	b := NewBBR(1200)
	b.rng = rand.New(rand.NewPCG(3, 4))
	var cut bool
	var at time.Time
	simulate(t, b, newLink(50e6, 50*time.Millisecond, 0.01), 5*time.Second, func(now time.Time) {
		if !b.baseline.learned {
			cut = cut || !math.IsInf(b.bwShortterm, 1)
		} else if at.IsZero() {
			at = now
			if !math.IsInf(b.bwShortterm, 1) || b.inflightShortterm != bbrInfinity {
				t.Fatalf("first baseline at %v left short-term bounds %.0f, %d", now.Sub(simStart), b.bwShortterm, b.inflightShortterm)
			}
		}
	})
	if !cut || at.IsZero() {
		t.Fatalf("cut before a baseline %v, baseline learned %v", cut, !at.IsZero())
	}
}

// Only the first baseline reports itself, again only once a stale one is
// dropped: a policer's baseline comes and goes, and each return would
// otherwise drop the bounds that hold BBR under it.
func TestLossBaselineReportsFirstOnly(t *testing.T) {
	noQueue := func(time.Duration) bool { return false }
	queue := func(time.Duration) bool { return true }
	var l lossBaseline
	learned := 0
	round := func(lost uint64, q func(time.Duration) bool) {
		l.noteLost(lost)
		l.noteRTT(time.Millisecond)
		if l.endRound(l.roundDelivered+1000-lost, true, q) {
			learned++
		}
	}
	round(0, noQueue)
	round(0, noQueue)
	for range bbrMinBaselineRounds {
		round(10, noQueue)
	}
	if learned != 1 || l.rate == 0 {
		t.Fatal("first baseline", learned, l.rate)
	}
	for range bbrBaselineRounds {
		round(0, noQueue)
	}
	round(10, noQueue)
	if l.rate == 0 || learned != 1 {
		t.Fatal("a baseline's return", learned, l.rate)
	}
	for range bbrStaleBaselineRounds {
		round(0, queue)
	}
	round(0, noQueue)
	for range bbrMinBaselineRounds {
		round(10, noQueue)
	}
	if learned != 2 {
		t.Fatal("after a stale baseline", learned, l.rate)
	}
}

func noQueueing(time.Duration) bool { return false }

// Where loss means congestion, the baseline stays at about zero throughout
// and BBR responds as draft-07 has it: a clean path, a shallow buffer, and a
// queue shared with NewReno flows, whose loss shows with a queue and is kept
// out, even when sixteen of them hold it standing past min_rtt's expiry. A
// policer is the exception it tolerates: cruising at the policed rate, BBR
// overruns the bucket now and then with no queue to show it, and learns a
// baseline that averages about 1% and peaks near 7%.
func TestLossBaselineStaysZeroUnderCongestion(t *testing.T) {
	for _, tc := range []struct {
		name   string
		link   func() *simLink
		renos  int
		d      time.Duration
		seeds  uint64 // BBR seeds 1 to this, or just 3 if 0
		maxTop float64
	}{
		{"clean", func() *simLink { return bufferedLink(100e6, 50*time.Millisecond, 1) }, 0, 60 * time.Second, 0, 0.002},
		{"policer", func() *simLink {
			l := bufferedLink(100e6, 50*time.Millisecond, 1)
			l.policer = &simPolicer{rate: 10e6 / 8, burst: 64 * 1024}
			return l
		}, 0, 60 * time.Second, 0, 0.08},
		{"shallow buffer", func() *simLink { return bufferedLink(100e6, 50*time.Millisecond, 0.1) }, 0, 60 * time.Second, 0, 0.002},
		// On a path this short, a round spans NewReno's sawtooth: its queue
		// overflows and drains within the round.
		{"with NewReno", func() *simLink { return bufferedLink(10e6, 20*time.Millisecond, 1) }, 2, 60 * time.Second, 8, 0.002},
		{"standing queue", func() *simLink { return bufferedLink(20e6, 50*time.Millisecond, 4) }, 16, 90 * time.Second, 0, 0.002},
	} {
		seeds := []uint64{3}
		if tc.seeds > 0 {
			seeds = nil
			for s := range tc.seeds {
				seeds = append(seeds, s+1)
			}
		}
		for _, seed := range seeds {
			b := newBBRFlow(seed)().(*BBR)
			top := 0.0
			flows := []*simFlow{{c: b, observe: func(time.Time) { top = max(top, b.baseline.rate) }}}
			for i := range tc.renos {
				flows = append(flows, &simFlow{c: newRenoFlow(), start: simStart.Add(3*time.Second + time.Duration(i)*100*time.Millisecond)})
			}
			simulateFlows(t, tc.link(), tc.d, flows...)
			t.Logf("%s, seed %d: baseline at most %.3f%%, %.3f%% at the end", tc.name, seed, 100*top, 100*b.baseline.rate)
			if top > tc.maxTop {
				t.Fatalf("%s, seed %d: baseline reached %.3f%%, want at most %.1f%%", tc.name, seed, 100*top, 100*tc.maxTop)
			}
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
		l.endRound(l.roundDelivered+950, true, noQueue)
	}
	if l.rate != 0.05 {
		t.Fatal("quiet rounds", l.rate)
	}
	// A round that ends as BBR starts to probe stays out.
	l.noteLost(500)
	l.noteRTT(time.Millisecond)
	l.endRound(l.roundDelivered+500, false, noQueue)
	if l.rate != 0.05 {
		t.Fatal("round ending in a probe", l.rate)
	}
	// So does the probing round.
	l.noteLost(500)
	l.endRound(l.roundDelivered+500, true, noQueue)
	if l.rate != 0.05 {
		t.Fatal("probing round", l.rate)
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
	// So is one whose lowest RTT shows none, if the first RTT after a loss
	// in it shows one: a queue that overflowed and drained within the round.
	over3ms := func(rtt time.Duration) bool { return rtt >= 3*time.Millisecond }
	l.noteRTT(time.Millisecond)
	l.noteLost(500)
	l.noteRTT(5 * time.Millisecond)
	l.endRound(l.roundDelivered+500, true, over3ms)
	if l.rate != 0.05 {
		t.Fatal("round with a queue at its loss", l.rate)
	}
	// Only that first sample counts: a later one with a queue, or one before
	// the loss, doesn't keep the round out.
	l.noteRTT(5 * time.Millisecond)
	l.noteLost(100)
	l.noteRTT(time.Millisecond)
	l.noteRTT(5 * time.Millisecond)
	l.endRound(l.roundDelivered+900, true, over3ms)
	if l.rate == 0.05 {
		t.Fatal("round with a queue away from its loss", l.rate)
	}
	rate := l.rate
	// A lossless quiet round lowers it; heavy loss is capped.
	l.endRound(l.roundDelivered+1000, true, noQueue)
	if l.rate >= rate {
		t.Fatal("lossless round", l.rate, rate)
	}
	for range bbrBaselineRounds {
		l.noteLost(900)
		l.endRound(l.roundDelivered+100, true, noQueue)
	}
	if l.rate != bbrMaxLossBaseline {
		t.Fatal("cap", l.rate)
	}
	// Rounds that all show a queue keep the baseline, until there have been
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

// The first RTT after a loss is judged at the round's end, against the
// path's lowest RTT by then: a sample that showed no queue against the
// reference when it came can show one against a lower RTT seen later.
func TestLossBaselineJudgesLossRTTAtRoundEnd(t *testing.T) {
	b := NewBBR(1200)
	b.baseline.endRound(0, true, b.queueing)
	b.baseline.endRound(0, true, b.queueing)
	b.baseline.noteRTT(60 * time.Millisecond)
	b.baseline.noteLost(100)
	b.baseline.noteRTT(60 * time.Millisecond)
	b.baseline.noteRTT(40 * time.Millisecond)
	b.baseline.endRound(b.baseline.roundDelivered+900, true, b.queueing)
	if b.baseline.count != 0 {
		t.Fatal("a round whose loss came 20 ms above the path's RTT joined the baseline")
	}
}

// Without a baseline any loss in a loss round is congestion, as draft-07
// has it. With one, the loss round's own loss, counted from its start,
// must exceed it by the threshold and by sampling noise: three standard
// deviations, wider for a short round, but at most bbrMaxLossMargin.
func TestRoundCongested(t *testing.T) {
	b := NewBBR(1200)
	// A loss round of packets, of which a share were lost, that began
	// after 5000 bytes were delivered.
	round := func(share float64, packets uint64) {
		lost := uint64(share * float64(packets*1200))
		b.lossRoundFrom, b.lossRoundLost, b.delivered = 5000, lost, 5000+packets*1200-lost
	}
	round(0.001, 1000)
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
		round(tc.rate, tc.packets)
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
	b.lossRoundFrom, b.lossRoundLost, b.delivered = 0, 0, 1200*1000
	if b.roundCongested() {
		t.Fatal("no loss or marks")
	}
	b.ceInRound = true
	if !b.roundCongested() {
		t.Fatal("CE marks with a baseline")
	}
}

// The estimate of where congestion crossed the threshold discounts the
// baseline from loss only: when the loss is within it, the marks alone
// cross at 2%, below the flight they were seen at.
func TestInflightAtCongestion(t *testing.T) {
	b := NewBBR(1200)
	b.baseline.rate = 0.05
	if got := b.inflightAtCongestion(1200, 120000, 0, 3600); got >= 120000 || got != inflightAtLoss(1200, 120000, 3600, bbrLossThresh) {
		t.Fatal("marks only:", got)
	}
	if got := b.inflightAtCongestion(1200, 120000, 9600, 1200); got != inflightAtLoss(1200, 120000, 10800, 0.07) {
		t.Fatal("loss above the baseline:", got)
	}
}

// A queue shows as an RTT above the path's lowest by an eighth, between 4
// and 16 ms; before any RTT, there is no telling, so it counts as one. The
// lowest RTT never expires: min_rtt, refreshed while another flow holds a
// standing queue, would let that queue pass for none.
func TestBBRQueueing(t *testing.T) {
	b := NewBBR(1200)
	if !b.queueing(50 * time.Millisecond) {
		t.Fatal("no RTT yet")
	}
	b.baseline.noteRTT(50 * time.Millisecond)
	for _, tc := range []struct {
		rtt  time.Duration
		want bool
	}{{55 * time.Millisecond, false}, {57 * time.Millisecond, true}, {0, true}} {
		if got := b.queueing(tc.rtt); got != tc.want {
			t.Fatalf("RTT %v over 50ms: queueing %v, want %v", tc.rtt, got, tc.want)
		}
	}
	b.minRTT = 80 * time.Millisecond
	b.baseline.noteRTT(80 * time.Millisecond)
	if !b.queueing(80 * time.Millisecond) {
		t.Fatal("an expired min_rtt resets the reference")
	}
	b = NewBBR(1200)
	b.baseline.noteRTT(400 * time.Millisecond)
	if b.queueing(415*time.Millisecond) || !b.queueing(416*time.Millisecond) {
		t.Fatal("16 ms cap")
	}
}

// On a buffer too shallow to show a queue, Startup still ends on loss: an
// overflow loses more than any baseline would discount. Under a queue
// requirement alone it ran on, dropping 6.9%, 14.7% and 4.7% of its first
// 3 s of packets on these paths, where it drops 3.2%, 7.2% and 2.2%.
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
