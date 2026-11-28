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
// more: 0.65-2.7 s at 50 ms and 3.1-3.6 s at 150 ms.
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
		rounds(b, 1, tc.rate)
		if startup := b.state == bbrStartup; startup != tc.startup {
			t.Fatalf("samples at %.2f of the estimate: Startup %v, want %v", tc.rate/1e6, startup, tc.startup)
		}
		if tc.startup && (b.fullBwReached || b.inflightLongterm != bbrInfinity || !math.IsInf(b.bwShortterm, 1)) {
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
	// A congestive cut marks congestion.
	b = cruising(now)
	b.lossInRound, b.bwLatest, b.inflightLatest = true, 5e5, 50_000
	b.adaptLowerBoundsFromCongestion()
	if !b.mini.congestedAt.Equal(now) {
		t.Fatal("a congestive cut didn't mark congestion")
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
