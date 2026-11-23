package transport

import "time"

// mtuProbeDeadline belongs to discovery, not QUIC loss detection. ACK, loss,
// write rejection and migration reset clear pending search state, making the
// old deadline inert without allowing it to time out a later probe.
func (a *pathApplication) mtuProbeDeadline() time.Time {
	if a.mtu == nil || !a.mtu.pending {
		return time.Time{}
	}
	return a.mtuTimeoutAt
}

// expireMTUProbe stops waiting for evidence of this size. It does not declare a
// packet lost, free flight bytes, alter congestion state, or rewind its number.
// Recovery still owns that packet and can acknowledge it later. The bounded
// search ledger permits late ACKs while the same candidate remains in use.
func (a *pathApplication) expireMTUProbe(now time.Time) bool {
	at := a.mtuProbeDeadline()
	if at.IsZero() || now.Before(at) {
		return false
	}
	a.mtuTimeoutAt = time.Time{}
	expired := a.mtu.lost(a.mtu.lastNumber)
	a.updateMTURaise(now)
	return expired
}

// completedLowerMTU identifies a settled search with room below the configured
// ceiling. Rechecking the ceiling itself cannot yield any larger usable size.
func (a *pathApplication) completedLowerMTU() bool {
	m := a.mtu
	return m != nil && !m.fixed && !m.pending && m.candidate == 0 && m.confirmed == m.upper && m.upper < m.ceiling
}

func (a *pathApplication) updateMTURaise(now time.Time) {
	if !a.completedLowerMTU() {
		a.mtuRaiseAt = time.Time{}
		return
	}
	if a.mtuRaiseAt.IsZero() {
		a.mtuRaiseAt = now.Add(10 * time.Minute)
	}
}

func (a *pathApplication) mtuRaiseDeadline() time.Time {
	if !a.completedLowerMTU() {
		return time.Time{}
	}
	return a.mtuRaiseAt
}

// expireMTURaise retains the confirmed size and packet-number high-water mark.
// It only reopens the upper search interval; evidence must come from fresh probes.
func (a *pathApplication) expireMTURaise(now time.Time) bool {
	at := a.mtuRaiseDeadline()
	if at.IsZero() || now.Before(at) {
		return false
	}
	a.mtu.upper = a.mtu.ceiling
	a.mtuRaiseAt = time.Time{}
	return true
}

func (a *pathApplication) resetMTU() {
	a.mtuNeedsValidation = false
	a.mtuChallenge = nil
	a.mtuBlackhole = mtuBlackholeState{floor: a.space.nextPN}
	if a.mtu != nil {
		a.mtu.reset()
	}
	a.mtuTimeoutAt, a.mtuRaiseAt = time.Time{}, time.Time{}
	// Retain rate limiting across tuple changes.
}
