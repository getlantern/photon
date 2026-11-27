package recovery

import "time"

// Careful Resume (draft-ietf-tsvwg-careful-resume-24) lets a sender that
// saved a path's congestion state, from an earlier connection to the same
// endpoint, jump toward that capacity instead of ramping up from the
// initial window: after one round confirms the path looks the same, it
// sends up to half the saved window, paced over the current RTT, and
// retreats to half of what was delivered if that brings congestion. BBR
// follows the draft's Appendix C: the jump happens in Startup, Startup's
// own exit validates it, and a congestion-triggered Drain is the retreat.
// The jump's window is half the saved window, as the draft's body has it,
// not that times Startup's gain, as its Appendix C does.

// ResumeState is a path's congestion state, saved for Careful Resume.
type ResumeState struct {
	// Bandwidth is the bottleneck bandwidth in bytes per second.
	Bandwidth float64
	MinRTT    time.Duration
	// Saved is when the state was measured, which the owner keeps; the
	// controller doesn't use it.
	Saved time.Time
}

// Resumer is a controller that can resume from saved state.
type Resumer interface {
	// Resume starts a connection's Careful Resume from saved state, with a
	// jump of at most maxJump bytes; zero sets no limit beyond half the
	// saved window. It does nothing once the connection has sent data.
	Resume(saved ResumeState, maxJump uint64)
	// Snapshot reports the path's state, to save, or false when it isn't
	// worth saving.
	Snapshot() (ResumeState, bool)
	// ResumeFailed reports whether a jump met congestion, so that the saved
	// state the connection resumed from should be dropped.
	ResumeFailed() bool
}

type crPhase uint8

const (
	crOff crPhase = iota
	// crReconnaissance: Startup from the initial window, watching the first
	// round for congestion and for an RTT unlike the saved one.
	crReconnaissance
	// crUnvalidated: the jump, for two rounds or until Startup ends.
	crUnvalidated
	// crRetreat: Drain after the jump met congestion.
	crRetreat
)

type carefulResume struct {
	phase   crPhase
	saved   ResumeState
	maxJump uint64
	// jump and rate are the jump's window and pacing rate.
	jump uint64
	rate float64
	// round is when the jump began; pipeStart and deliveredStart the bytes
	// in flight and delivered then, from which PipeSize grows.
	round          uint64
	pipeStart      uint64
	deliveredStart uint64
	retreatCwnd    uint64
	failed         bool
	// measured is when the path's bandwidth was last measured, which a
	// snapshot reports as its Saved time.
	measured time.Time
}

// bbrSnapshotMinWindows is the least saved window, in initial windows,
// worth saving: the draft lets a sender skip saving below four.
const bbrSnapshotMinWindows = 4

// Resume implements Resumer.
func (b *BBR) Resume(saved ResumeState, maxJump uint64) {
	if b.state != bbrStartup || b.delivered > 0 || b.inflight > 0 || saved.Bandwidth <= 0 || saved.MinRTT <= 0 {
		return
	}
	b.cr = carefulResume{phase: crReconnaissance, saved: saved, maxJump: maxJump}
}

// Snapshot implements Resumer. It saves the bandwidth and minimum RTT once
// Startup has ended, so never an overshoot, and not in ProbeRTT, whose
// samples understate the path, nor while retreating from a failed jump. Its
// Saved time is when the bandwidth was last measured, so that the state of
// a connection that idled after measuring the path expires on time.
func (b *BBR) Snapshot() (ResumeState, bool) {
	if !b.fullBwReached || b.state == bbrProbeRTT || b.cr.phase == crRetreat || b.maxBw <= 0 || b.minRTT == time.Duration(1<<63-1) {
		return ResumeState{}, false
	}
	if b.maxBw*b.minRTT.Seconds() < float64(bbrSnapshotMinWindows*b.initialWindow) {
		return ResumeState{}, false
	}
	return ResumeState{Bandwidth: b.maxBw, MinRTT: b.minRTT, Saved: b.cr.measured}, true
}

// ResumeFailed implements Resumer.
func (b *BBR) ResumeFailed() bool { return b.cr.failed }

// crOnACK advances Careful Resume on an ACK, after the model's update and
// before the window and pacing rate are set.
func (b *BBR) crOnACK(rs RateSample) {
	switch b.cr.phase {
	case crReconnaissance:
		if b.state != bbrStartup {
			b.cr.phase = crOff
			return
		}
		// The initial window acknowledged without congestion, while sending
		// as fast as the window allows: a rate-limited sender stays here.
		if b.delivered < b.initialWindow || rs.AppLimited {
			return
		}
		rtt, saved := b.minRTT, b.cr.saved.MinRTT
		if rtt <= saved/2 || rtt > 10*saved {
			// The path has changed.
			b.cr.phase = crOff
			return
		}
		jump := uint64(b.cr.saved.Bandwidth*saved.Seconds()) / 2
		if b.cr.maxJump > 0 {
			jump = min(jump, b.cr.maxJump)
		}
		if jump <= b.cwnd {
			b.cr.phase = crOff
			return
		}
		b.cr.phase, b.cr.jump, b.cr.rate = crUnvalidated, jump, float64(jump)/rtt.Seconds()
		b.cr.round, b.cr.pipeStart, b.cr.deliveredStart = b.roundCount, b.inflight, b.delivered
	case crUnvalidated:
		// Startup's own exit validates the jump; otherwise it ends after
		// two rounds, as the draft's Appendix C has it.
		if b.state != bbrStartup || b.roundCount >= b.cr.round+2 {
			b.cr.phase = crOff
		}
	case crRetreat:
		if b.state != bbrDrain {
			b.cr.phase = crOff
		}
	}
}

// crCongestion answers loss or ECN-CE. In Reconnaissance it ends Careful
// Resume, and normal congestion control continues. After the jump it is
// Safe Retreat: Startup ends in Drain, and the window falls to half of
// PipeSize, what the path was seen to carry, and grows no more until Drain
// ends.
func (b *BBR) crCongestion() {
	switch b.cr.phase {
	case crReconnaissance:
		b.cr.phase = crOff
	case crUnvalidated:
		pipe := b.cr.pipeStart + (b.delivered - min(b.delivered, b.cr.deliveredStart))
		b.cr.phase, b.cr.failed = crRetreat, true
		b.cr.retreatCwnd = max(pipe/2, b.minPipeCwnd())
		b.fullBwReached = true
		b.enterDrain()
		// Drain paces from now, a loss its timer found included, and the
		// window recovery would restore is the jump's no more.
		b.setPacingRate()
		b.cwnd = min(b.cwnd, b.cr.retreatCwnd)
		b.priorCwnd = min(b.priorCwnd, b.cr.retreatCwnd)
	}
}

// crBound applies Careful Resume to the window and pacing rate the model
// set: the jump's, or Safe Retreat's cap.
func (b *BBR) crBound() {
	switch b.cr.phase {
	case crUnvalidated:
		b.cwnd = max(b.cwnd, b.cr.jump)
		b.pacingRate = max(b.pacingRate, b.cr.rate)
	case crRetreat:
		b.cwnd = min(b.cwnd, b.cr.retreatCwnd)
	}
}
