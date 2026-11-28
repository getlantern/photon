package recovery

import "time"

// BBRv3 probes for bandwidth every two to three seconds, so that a Reno or
// CUBIC flow on the bottleneck can grow its window between probes; a link
// whose rate rose meanwhile is found only at the next probe, seconds on.
// BBRv1 probed every eight rounds, a round at 1.25 times the bandwidth
// estimate followed by a round at 0.75, which averages to the estimate and
// so takes no more from other flows, but finds spare capacity within a few
// hundred milliseconds. A mini-probe borrows that: cruising BBR spends a
// round at 1.25 and one at 0.75 every bbrMiniProbeRounds, and if the extra
// round's packets came back faster than the estimate by bbrMiniProbeGrowth,
// the path has more to give, and BBR starts its full probe now instead of
// waiting. Otherwise nothing in its model changes.
const (
	bbrMiniProbeRounds = 8
	bbrMiniProbeGrowth = 1.1
)

type miniPhase uint8

const (
	miniOff  miniPhase = iota // cruising, counting rounds
	miniUp                    // the round at 1.25
	miniDown                  // the round at 0.75, as the up round's packets come back
)

type miniProbe struct {
	phase  miniPhase
	rounds uint64  // since cruising began or the last mini-probe ended
	maxBw  float64 // the highest delivery rate measured during it
	fromBw float64 // the bandwidth estimate as it began, which its samples raise
	// congestedAt is when congestion last cut the short-term bounds.
	congestedAt time.Time
}

// bbrMiniProbeQuiet is how long after congestion last cut the short-term
// bounds mini-probes stay off: a path that shows congestion has another
// flow on it, whose share mini-probes would press on.
const bbrMiniProbeQuiet = 10 * time.Second

// quietState reports whether BBR is neither probing nor mini-probing, for
// the loss baseline.
func (b *BBR) quietState() bool {
	return (b.state == bbrProbeBWDown || b.state == bbrProbeBWCruise) && b.mini.phase == miniOff
}

// updateMiniProbe runs on each ACK in ProbeBW_CRUISE.
func (b *BBR) updateMiniProbe(rs RateSample) {
	if b.mini.phase != miniOff {
		if b.lossInRound || rs.CE > 0 || rs.AppLimited {
			// Contention or an idle sender: no reading, and no probe.
			b.endMiniProbe()
			return
		}
		if rs.Valid {
			b.mini.maxBw = max(b.mini.maxBw, float64(rs.Rate))
		}
	}
	if !b.roundStart {
		return
	}
	switch b.mini.phase {
	case miniOff:
		quiet := b.mini.congestedAt.IsZero() || b.now.Sub(b.mini.congestedAt) > bbrMiniProbeQuiet
		if b.mini.rounds++; b.mini.rounds >= bbrMiniProbeRounds && !rs.AppLimited && quiet {
			b.mini.phase, b.mini.maxBw, b.mini.fromBw = miniUp, 0, b.bw
			b.pacingGain = 1.25
		}
	case miniUp:
		b.mini.phase = miniDown
		b.pacingGain = 0.75
	case miniDown:
		grew := b.mini.maxBw > bbrMiniProbeGrowth*b.mini.fromBw
		b.endMiniProbe()
		if grew {
			b.restartStartup()
		}
	}
}

// bbrRiseGrowth is how much faster than the estimate it began from a
// probe's packets must come back to show the path's rate has risen, not
// just that it has room: Startup's own test of growth.
const bbrRiseGrowth = 1.25

// upFoundRise reports whether ProbeBW_UP, on an uncontended path, has seen
// its packets come back faster than the estimate it began from by
// bbrRiseGrowth, as a path whose rate rose by more than a probe's gain
// does: then Startup finds the new rate in far fewer rounds than UP.
func (b *BBR) upFoundRise(rs RateSample) bool {
	if rs.Valid && !rs.AppLimited {
		b.mini.maxBw = max(b.mini.maxBw, float64(rs.Rate))
	}
	quiet := b.mini.congestedAt.IsZero() || b.now.Sub(b.mini.congestedAt) > bbrMiniProbeQuiet
	return b.roundStart && quiet && !b.lossInRound && b.mini.fromBw > 0 && b.mini.maxBw > bbrRiseGrowth*b.mini.fromBw
}

// restartStartup re-enters Startup after a mini-probe found the path's rate
// had risen. ProbeBW_UP raises its in-flight limit additively, a few
// packets more each round, so it climbs to a rate several times the old
// one over many rounds: seconds on a long path. Startup grows the rate by
// its gain each round until it stops growing, as it does on a new path; the
// old in-flight limit goes, as it describes the slower path.
func (b *BBR) restartStartup() {
	b.checkProbeLoss, b.prevProbeTooHigh, b.prevProbePrecautionary = false, false, false
	b.fullBwReached = false
	b.resetFullBW()
	b.inflightLongterm = bbrInfinity
	b.resetShortTermModel()
	b.enterStartup()
}

func (b *BBR) endMiniProbe() {
	b.mini = miniProbe{congestedAt: b.mini.congestedAt}
	b.pacingGain = 1
}
