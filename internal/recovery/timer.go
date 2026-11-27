package recovery

import "time"

// TimerState supplies connection facts that packet recovery cannot infer.
// PeerValidated means the peer has validated our address: always true on a
// server, and true on a client after a Handshake ACK or handshake confirmation.
type TimerState struct {
	PeerValidated, HandshakeKeys, Confirmed, AmplificationBlocked bool
	MaxACKDelay                                                   time.Duration
}

// Timer implements the single RFC 9002 loss/PTO timer. The owner calls Arm
// after send, ACK, key discard, or changes to address validation/send credit.
// Merely reading Deadline must not restart an anti-deadlock timer.
type Timer struct {
	at           time.Time
	space        Space
	loss         bool
	antiDeadlock bool
	backoff      uint32
}

type Timeout struct {
	Space Space
	Lost  []Packet
	// Probe requests one ack-eliciting packet, using new or outstanding data,
	// or PING if neither is available. PTO does not declare outstanding data lost.
	Probe bool
}

func (t *Timer) Deadline() time.Time { return t.at }
func (t *Timer) Backoff() uint32     { return t.backoff }

// Progress resets exponential backoff only for a newly acknowledged packet
// once peer address validation is known. Duplicate ACKs are not progress.
func (t *Timer) Progress(events Events, peerValidated bool) {
	if peerValidated && len(events.Acked) > 0 {
		t.backoff = 0
	}
}

// DiscardKeys resets the timer and backoff at an encryption-level retirement.
// The caller then invokes Arm with the remaining packet spaces.
func (t *Timer) DiscardKeys() { t.at = time.Time{}; t.backoff = 0 }

func (t *Timer) Arm(r *Sent, now time.Time, state TimerState) {
	oldAt, oldSpace, oldAnti := t.at, t.space, t.antiDeadlock
	t.at = time.Time{}
	t.loss = false
	t.antiDeadlock = false
	if at, space, ok := r.LossTime(); ok {
		t.at, t.space, t.loss = at, space, true
		return
	}
	if state.AmplificationBlocked {
		return
	}
	anyEliciting := false
	for i := range r.spaces {
		s := &r.spaces[i]
		if s.eliciting == 0 {
			continue
		}
		anyEliciting = true
		if Space(i) == Application && !state.Confirmed {
			continue
		}
		at := s.lastEliciting.Add(r.RTT.PTO(Space(i) == Application, state.Confirmed, state.MaxACKDelay, t.backoff))
		if t.at.IsZero() || at.Before(t.at) {
			t.at, t.space = at, Space(i)
		}
	}
	if !anyEliciting && !state.PeerValidated {
		t.antiDeadlock = true
		t.space = Initial
		if state.HandshakeKeys {
			t.space = Handshake
		}
		t.at = now.Add(r.RTT.PTO(false, false, 0, t.backoff))
		// Re-evaluating unchanged state (for example after an unrelated
		// datagram) must not postpone the client's anti-deadlock probe.
		if oldAnti && oldSpace == t.space && !oldAt.IsZero() && oldAt.Before(t.at) {
			t.at = oldAt
		}
	}
}

// Expire consumes a due timer exactly once. The owner sends requested probes
// before rearming, so the new deadline uses the actual probe transmission time.
func (t *Timer) Expire(r *Sent, now time.Time) (Timeout, bool) {
	if t.at.IsZero() || now.Before(t.at) {
		return Timeout{}, false
	}
	t.at = time.Time{}
	event := Timeout{Space: t.space}
	if t.loss {
		event.Lost = r.DetectLoss(t.space, now)
	} else {
		event.Probe = true
		if t.backoff < 63 {
			t.backoff++
		}
	}
	return event, true
}
