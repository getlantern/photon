package recovery

import (
	"math"
	"testing"
	"time"
)

// When the link's rate rises fivefold, BBR finds it within a second at
// 50 ms and two and a half at 150 ms: a mini-probe sees its packets come
// back faster than the estimate, or a probe's UP sees them come back faster
// than a probe's gain, and BBR re-enters Startup. BBRv3 alone waited for
// its next probe, two to three seconds on, and climbed in UP for seconds
// more: 0.65-2.7 s at 50 ms and 3.2-3.8 s at 150 ms.
func TestBBRFindsRateRise(t *testing.T) {
	for _, tc := range []struct {
		rtt, within time.Duration
	}{{50 * time.Millisecond, time.Second}, {150 * time.Millisecond, 2500 * time.Millisecond}} {
		link := bufferedLink(50e6, tc.rtt, 1)
		link.rate = func(now time.Time) float64 {
			if now.Sub(simStart) < 3*time.Second {
				return 10e6 / 8
			}
			return 50e6 / 8
		}
		b := newBBRFlow(3)().(*BBR)
		var found time.Duration
		simulate(t, b, link, 8*time.Second, func(now time.Time) {
			if found == 0 && b.maxBw > 0.8*50e6/8 {
				found = now.Sub(simStart) - 3*time.Second
			}
		})
		t.Logf("%v: found 80%% of the new rate %v after the rise", tc.rtt, found)
		if found <= 0 || found > tc.within {
			t.Fatalf("%v: found 80%% of the new rate %v after the rise, want within %v", tc.rtt, found, tc.within)
		}
	}
}

// cruising returns a BBR cruising at 1 MB/s, as of now.
func cruising(now time.Time) *BBR {
	b := NewBBR(1200)
	b.now, b.fullBwReached, b.state = now, true, bbrProbeBWCruise
	b.bw, b.maxBw, b.inflightLongterm = 1e6, 1e6, 100_000
	b.pacingGain = 1
	return b
}

// rounds runs n ACKs that each start a round, with rate samples of rate.
func rounds(b *BBR, n int, rate float64) {
	for range n {
		b.roundStart = true
		b.updateMiniProbe(RateSample{Valid: true, Rate: uint64(rate)})
	}
}

// Every bbrMiniProbeRounds of cruising, a mini-probe spends a round at 1.25
// and one at 0.75; if its packets came back faster than the estimate it
// began from by bbrMiniProbeGrowth, BBR re-enters Startup, its in-flight
// limit gone, and otherwise goes on cruising. Samples raise the estimate as
// they come, so the test is against where it began.
func TestMiniProbe(t *testing.T) {
	now := time.Unix(100, 0)
	for _, tc := range []struct {
		rate    float64
		startup bool
	}{{1.05e6, false}, {1.2e6, true}} {
		b := cruising(now)
		rounds(b, bbrMiniProbeRounds-1, 1e6)
		if b.mini.phase != miniOff || b.pacingGain != 1 {
			t.Fatal("mini-probe before", bbrMiniProbeRounds, "rounds")
		}
		rounds(b, 1, 1e6)
		if b.mini.phase != miniUp || b.pacingGain != 1.25 {
			t.Fatal("no mini-probe after", bbrMiniProbeRounds, "rounds", b.mini.phase, b.pacingGain)
		}
		rounds(b, 1, 1e6)
		if b.mini.phase != miniDown || b.pacingGain != 0.75 {
			t.Fatal("no drain round", b.mini.phase, b.pacingGain)
		}
		b.bw = tc.rate // as the samples raise it
		b.bwShortterm, b.inflightShortterm = 5e5, 50_000
		rounds(b, 1, tc.rate)
		if startup := b.state == bbrStartup; startup != tc.startup {
			t.Fatalf("samples at %.2f of the estimate: Startup %v, want %v", tc.rate/1e6, startup, tc.startup)
		}
		if tc.startup && (b.fullBwReached || b.inflightShortterm != bbrInfinity || b.inflightLongterm != bbrInfinity || !math.IsInf(b.bwShortterm, 1)) {
			t.Fatal("Startup kept the old model")
		}
		if !tc.startup && (b.mini.phase != miniOff || b.pacingGain != 1) {
			t.Fatal("not cruising after a mini-probe that found nothing")
		}
	}
}

// Loss or ECN-CE ends a mini-probe with no reading, and congestion that cut
// the short-term bounds keeps mini-probes off for bbrMiniProbeQuiet: a path
// with another flow on it doesn't get them.
func TestMiniProbeContention(t *testing.T) {
	now := time.Unix(100, 0)
	b := cruising(now)
	rounds(b, bbrMiniProbeRounds, 1e6)
	b.lossInRound = true
	b.updateMiniProbe(RateSample{Valid: true, Rate: 2e6})
	if b.mini.phase != miniOff || b.pacingGain != 1 {
		t.Fatal("loss didn't end the mini-probe")
	}
	b.lossInRound = false
	b.mini.congestedAt = now.Add(-bbrMiniProbeQuiet / 2)
	rounds(b, 2*bbrMiniProbeRounds, 1e6)
	if b.mini.phase != miniOff {
		t.Fatal("mini-probe within", bbrMiniProbeQuiet, "of congestion")
	}
	b.mini.congestedAt = now.Add(-2 * bbrMiniProbeQuiet)
	rounds(b, 1, 1e6)
	if b.mini.phase != miniUp {
		t.Fatal("no mini-probe long after congestion")
	}
	// A congestive cut marks congestion if it lost a packet sent after
	// Startup and Drain, or saw ECN-CE in a loss round that began after
	// them; not for Startup's own losses.
	for _, tc := range []struct {
		name                                    string
		lostSinceStartup, ce, roundAfterStartup bool
		marked                                  bool
	}{
		{"loss of a packet sent after Startup", true, false, false, true},
		{"loss of a packet sent in Startup", false, false, true, false},
		{"ECN-CE in a round begun after Startup", false, true, true, true},
		{"ECN-CE in a round begun in Startup", false, true, false, false},
	} {
		b = cruising(now)
		b.lossInRound, b.ceInRound, b.lostSinceStartup, b.lossRoundAfterStartup = true, tc.ce, tc.lostSinceStartup, tc.roundAfterStartup
		b.bwLatest, b.inflightLatest = 5e5, 50_000
		b.adaptLowerBoundsFromCongestion()
		if marked := b.mini.congestedAt.Equal(now); marked != tc.marked {
			t.Fatalf("%s: congestion marked %v, want %v", tc.name, marked, tc.marked)
		}
	}
	// A loss notes whether its packet was sent after Startup and Drain.
	for _, sentAfter := range []bool{true, false} {
		b = cruising(now)
		b.startupDoneAt = now.Add(-time.Second)
		sent := now.Add(-time.Second / 2)
		if !sentAfter {
			sent = now.Add(-2 * time.Second)
		}
		b.OnLoss(LossEvent{Lost: []Packet{{Bytes: 1200, InFlight: true, SentAt: sent}}, TotalLost: 1200, Now: now})
		if b.lostSinceStartup != sentAfter {
			t.Fatalf("a packet sent after Startup %v: noted %v", sentAfter, b.lostSinceStartup)
		}
	}
}

// ProbeBW_UP on an uncontended path re-enters Startup once its packets come
// back faster than the estimate it began from by bbrRiseGrowth; with loss
// in the round, or recent congestion, it doesn't.
func TestUpFoundRise(t *testing.T) {
	now := time.Unix(100, 0)
	for _, tc := range []struct {
		name      string
		rate      float64
		loss      bool
		congested bool
		want      bool
	}{
		{"a probe's gain", 1.2e6, false, false, false},
		{"a rise", 1.4e6, false, false, true},
		{"a rise with loss", 1.4e6, true, false, false},
		{"a rise after congestion", 1.4e6, false, true, false},
	} {
		b := cruising(now)
		b.state, b.mini.fromBw, b.lossInRound, b.roundStart = bbrProbeBWUp, 1e6, tc.loss, true
		if tc.congested {
			b.mini.congestedAt = now
		}
		if got := b.upFoundRise(RateSample{Valid: true, Rate: uint64(tc.rate)}); got != tc.want {
			t.Fatalf("%s: found %v, want %v", tc.name, got, tc.want)
		}
	}
}

// A mini-probe's rounds aren't quiet, so their loss stays out of the loss
// baseline, as a full probe's does; and its round at 1.25 may fill the
// in-flight limit, as UP does, rather than cruising's limit less headroom.
func TestMiniProbeRoundsAreProbes(t *testing.T) {
	now := time.Unix(100, 0)
	for _, phase := range []miniPhase{miniUp, miniDown} {
		b := cruising(now)
		b.mini.phase, b.baseline.roundQuiet, b.roundStart = phase, true, true
		b.noteRoundStartState()
		if b.baseline.roundQuiet {
			t.Fatalf("mini-probe phase %d counted as quiet", phase)
		}
	}
	for _, tc := range []struct {
		phase miniPhase
		want  uint64
	}{{miniOff, 85_000}, {miniUp, 100_000}} {
		b := cruising(now)
		b.mini.phase, b.cwnd = tc.phase, 1_000_000
		b.boundCwndForModel()
		if b.cwnd != tc.want {
			t.Fatalf("mini-probe phase %d: window %d, want %d", tc.phase, b.cwnd, tc.want)
		}
	}
}

// ECN-CE or an app-limited sample ends a mini-probe with no reading, as
// loss does.
func TestMiniProbeAborts(t *testing.T) {
	now := time.Unix(100, 0)
	for _, rs := range []RateSample{{Valid: true, Rate: 2e6, CE: 1}, {Valid: true, Rate: 2e6, AppLimited: true}} {
		b := cruising(now)
		rounds(b, bbrMiniProbeRounds, 1e6)
		b.updateMiniProbe(rs)
		if b.mini.phase != miniOff || b.pacingGain != 1 || b.mini.maxBw != 0 {
			t.Fatalf("%+v didn't end the mini-probe", rs)
		}
	}
}

// Re-entering Startup from UP drops UP's loss check, so Startup's overshoot
// doesn't set a new in-flight limit for the faster path; and ProbeRTT ends a
// mini-probe.
func TestRestartAndProbeRTTClearProbeState(t *testing.T) {
	b := cruising(time.Unix(100, 0))
	b.state, b.checkProbeLoss, b.prevProbeTooHigh, b.prevProbePrecautionary = bbrProbeBWUp, true, true, true
	b.restartStartup()
	if b.checkProbeLoss || b.prevProbeTooHigh || b.prevProbePrecautionary {
		t.Fatal("Startup kept UP's loss check")
	}
	b = cruising(time.Unix(100, 0))
	b.mini.phase = miniUp
	b.enterProbeRTT()
	if b.mini.phase != miniOff {
		t.Fatal("ProbeRTT kept the mini-probe")
	}
}

// Loss in the current loss round keeps a mini-probe from starting. The ACK
// that ends a loss round clears its loss before the rise checks run; they
// still see it, so a lossy round lets neither a mini-probe nor UP restart
// Startup.
func TestRiseChecksSeeEndedLossRound(t *testing.T) {
	now := time.Unix(100, 0)
	lossy := func(b *BBR, prior uint64) {
		b.cycleStamp, b.bwProbeWait = now, 3*time.Second
		b.bdp, b.cwnd, b.minRTT = 50_000, 100_000, 50*time.Millisecond
		b.delivered, b.lossRoundDelivered, b.nextRoundDelivered = 10_000_000, 9_000_000, 9_000_000
		b.OnLoss(LossEvent{Lost: []Packet{{Bytes: 1200, InFlight: true}}, TotalLost: 1200, TotalDelivered: 10_000_000, Flight: 50_000, Now: now})
		b.OnACK(ACKEvent{Rate: RateSample{Valid: true, Rate: 2e6, NewlyAcked: 1200, PriorDelivered: prior, TotalDelivered: 10_001_200, Delivered: 50_000, RTT: 50 * time.Millisecond}, Flight: 50_000, SmoothedRTT: 50 * time.Millisecond, Now: now})
	}
	// A round starts, but not a loss round.
	b := cruising(now)
	b.mini.rounds = bbrMiniProbeRounds - 1
	lossy(b, 9_500_000)
	if b.state != bbrProbeBWCruise || b.mini.phase != miniOff {
		t.Fatalf("lossy round: state %d, mini-probe phase %d", b.state, b.mini.phase)
	}
	// Both end.
	b = cruising(now)
	b.mini.phase, b.mini.fromBw = miniDown, 1e6
	lossy(b, 10_000_000)
	if b.state == bbrStartup {
		t.Fatal("a mini-probe restarted Startup on a lossy round")
	}
	b = cruising(now)
	b.state, b.mini.fromBw, b.pacingGain = bbrProbeBWUp, 1e6, 1.25
	lossy(b, 10_000_000)
	if b.state == bbrStartup {
		t.Fatal("UP restarted Startup on a lossy round")
	}
}

// A loss found by a timer ends a mini-probe at once, pacing and windowing
// as cruising does, since sends can follow with no ACK.
func TestMiniProbeTimerLossEndsIt(t *testing.T) {
	b := cruising(time.Unix(100, 0))
	b.minRTT, b.cwnd = 100*time.Millisecond, 1_000_000
	rounds(b, bbrMiniProbeRounds, 1e6)
	b.pacingRate = 1.25e6
	b.OnLoss(LossEvent{Lost: []Packet{{Bytes: 1200, InFlight: true}}, TotalLost: 1200, Now: time.Unix(100, 0)})
	if b.mini.phase != miniOff || b.pacingGain != 1 {
		t.Fatal("timer loss didn't end the mini-probe")
	}
	if b.pacingRate >= 1.25e6 || b.cwnd > b.inflightWithHeadroom() {
		t.Fatal("still probing: pacing", b.pacingRate, "window", b.cwnd)
	}
}

// A peak UP measures in a round that turns out congested is dropped, so a later clean round at
// the old rate doesn't restart Startup on it; a clean rise still does.
func TestUpDropsCongestedPeak(t *testing.T) {
	now := time.Unix(100, 0)
	b := cruising(now)
	b.state, b.mini.fromBw = bbrProbeBWUp, 1e6
	b.roundStart = false
	b.upFoundRise(RateSample{Valid: true, Rate: 1.4e6}) // before loss shows
	b.lossInRound = true
	b.upFoundRise(RateSample{Valid: true, Rate: 1.4e6})
	b.lossInRound, b.roundStart = false, true
	if b.upFoundRise(RateSample{Valid: true, Rate: 1e6}) {
		t.Fatal("restarted on a peak measured under congestion")
	}
	if !b.upFoundRise(RateSample{Valid: true, Rate: 1.4e6}) {
		t.Fatal("no restart on a clean rise")
	}
}

// A reordered ACK can report loss or ECN-CE from its packet's flight after
// the loss round that held it has ended; UP drops that sample and the peak
// before it, and a mini-probe neither starts nor goes on with it.
func TestRiseChecksRejectCongestedSamples(t *testing.T) {
	now := time.Unix(100, 0)
	for _, rs := range []RateSample{{Valid: true, Rate: 1.4e6, CE: 1200}, {Valid: true, Rate: 1.4e6, Lost: 1200}} {
		b := cruising(now)
		b.state, b.mini.fromBw = bbrProbeBWUp, 1e6
		b.upFoundRise(RateSample{Valid: true, Rate: 1.4e6})
		b.upFoundRise(rs)
		b.roundStart = true
		if b.upFoundRise(RateSample{Valid: true, Rate: 1e6}) {
			t.Fatalf("UP restarted on a peak before %+v", rs)
		}
		b = cruising(now)
		rounds(b, bbrMiniProbeRounds, 1e6)
		b.updateMiniProbe(rs)
		if b.mini.phase != miniOff {
			t.Fatalf("%+v didn't end the mini-probe", rs)
		}
		b = cruising(now)
		rounds(b, bbrMiniProbeRounds-1, 1e6)
		b.roundStart = true
		b.updateMiniProbe(rs)
		if b.mini.phase != miniOff || b.pacingGain != 1 {
			t.Fatalf("%+v started a mini-probe", rs)
		}
		rounds(b, 1, 1e6)
		if b.mini.phase != miniUp {
			t.Fatal("no mini-probe on the next clean round")
		}
	}
}

// Startup's overshoot, whose losses come to light once BBR is cruising and
// cut the short-term bounds on a shallow buffer, doesn't keep mini-probes
// off: they show BBR's own queue, not another flow's. A NewReno flow
// joining does.
func TestStartupOvershootLeavesMiniProbesOn(t *testing.T) {
	for _, seed := range []uint64{3, 7, 11} {
		b := newBBRFlow(seed)().(*BBR)
		cut, probed := false, false
		simulate(t, b, bufferedLink(50e6, 50*time.Millisecond, 0.25), 3*time.Second, func(time.Time) {
			cut = cut || !math.IsInf(b.bwShortterm, 1)
			probed = probed || b.mini.phase != miniOff
		})
		t.Logf("seed %d: short-term cut %v, congestion marked %v, mini-probed %v", seed, cut, !b.mini.congestedAt.IsZero(), probed)
		if !cut || !b.mini.congestedAt.IsZero() || !probed {
			t.Fatalf("seed %d: short-term cut %v, Startup's losses marked congestion %v, mini-probed %v; want a cut, no mark and mini-probes", seed, cut, !b.mini.congestedAt.IsZero(), probed)
		}
	}
	b := newBBRFlow(3)().(*BBR)
	simulateFlows(t, bufferedLink(20e6, 50*time.Millisecond, 1), 20*time.Second,
		&simFlow{c: b}, &simFlow{c: newRenoFlow(), start: simStart.Add(3 * time.Second)})
	if b.mini.congestedAt.IsZero() {
		t.Fatal("a NewReno flow on the path didn't mark congestion")
	}
}

// Startup ends, for congestion's evidence, when BBR enters ProbeBW: it
// stays unset through Startup, Drain and a ProbeRTT that interrupts them,
// later probes and ProbeRTT don't move it, and entering Startup again
// clears it.
func TestStartupDoneAt(t *testing.T) {
	now := time.Unix(100, 0)
	b := cruising(now)
	b.enterStartup()
	b.enterProbeRTT()
	if !b.startupDoneAt.IsZero() {
		t.Fatal("Startup done in Startup or a ProbeRTT that interrupted it")
	}
	b.startProbeBWDown()
	if !b.startupDoneAt.Equal(now) {
		t.Fatal("Startup not done in ProbeBW")
	}
	b.now = now.Add(time.Second)
	b.startProbeBWCruise()
	b.enterProbeRTT()
	b.startProbeBWDown()
	if !b.startupDoneAt.Equal(now) {
		t.Fatal("ProbeRTT and a later ProbeBW moved Startup's end")
	}
	b.enterStartup()
	if !b.startupDoneAt.IsZero() {
		t.Fatal("entering Startup left Startup done")
	}
}

// ECN-CE that begins a loss round after Startup notes that the round began
// after it, so a cut on its marks alone arms the guard.
func TestCEStartsLossRoundAfterStartup(t *testing.T) {
	now := time.Unix(100, 0)
	for _, after := range []bool{true, false} {
		b := cruising(now)
		b.delivered, b.lossRoundDelivered, b.nextRoundDelivered = 100_000, 200_000, 200_000
		b.lossRoundAfterStartup = !after
		if after {
			b.startupDoneAt = now.Add(-time.Second)
		}
		b.OnACK(ACKEvent{Rate: RateSample{NewlyAcked: 1200, NewCE: 1200, CE: 1200, PriorDelivered: 90_000, TotalDelivered: 101_200}, Flight: 50_000, Now: now})
		if !b.lossInRound || b.lossRoundAfterStartup != after {
			t.Fatalf("CE after Startup %v: loss round %v, began after Startup %v", after, b.lossInRound, b.lossRoundAfterStartup)
		}
	}
}
