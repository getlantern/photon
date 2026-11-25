package transport

import "time"

// RFC 9000 section 13.2.1 lets a receiver hold the ACK for an ack-eliciting
// application packet for up to its advertised max_ack_delay, but it SHOULD
// acknowledge at once after every second one, and at once for a packet that
// arrives out of order (a duplicate always does, so its ACK may replace a lost
// one) or marked ECN-CE. Initial and Handshake packets are acknowledged at
// once. A max_ack_delay of zero makes every deadline already due. Acknowledging every packet instead roughly doubles the
// datagrams a bulk transfer sends in the reverse direction.
const ackElicitingThreshold = 2

// scheduleACK records an ack-eliciting packet the space has just recorded and
// decides when to acknowledge it: now (ackPending), or by ackDueAt. previous is
// the largest packet number recorded before it, if any.
func (s *numberSpace) scheduleACK(space int, pn, previous uint64, hadPrevious bool, marking ECN, now time.Time, delay time.Duration) {
	if s.ackPending {
		return
	}
	s.unackedEliciting++
	outOfOrder := hadPrevious && pn != previous+1
	if space != applicationSpace || outOfOrder || marking == ECNCE || s.unackedEliciting >= ackElicitingThreshold {
		s.ackPending = true
		s.ackDueAt = time.Time{}
		return
	}
	if s.ackDueAt.IsZero() {
		s.ackDueAt = now.Add(delay)
	}
}

// clearACK forgets any pending or delayed ACK, once an ACK is sent or the space
// stops acknowledging.
func (s *numberSpace) clearACK() {
	s.ackPending = false
	s.unackedEliciting = 0
	s.ackDueAt = time.Time{}
}

// expireACKDelay makes a delayed ACK pending once it is due.
func (s *numberSpace) expireACKDelay(now time.Time) {
	if !s.ackDueAt.IsZero() && !now.Before(s.ackDueAt) {
		s.ackPending = true
		s.ackDueAt = time.Time{}
	}
}

// ackDelay is how long an application ACK may wait: the max_ack_delay this
// endpoint advertises, less a millisecond for the owner's timer granularity.
func (c *Conn) ackDelay() time.Duration {
	return time.Duration(c.config.Parameters.MaxAckDelay)*time.Millisecond - time.Millisecond
}

// ackDeadline is the earliest delayed application ACK, zero if none.
func (c *Conn) ackDeadline() time.Time {
	at := c.spaces[applicationSpace].ackDueAt
	if c.multipath != nil {
		for _, state := range c.multipath.paths {
			if app := state.application; app != nil && !app.space.ackDueAt.IsZero() && (at.IsZero() || app.space.ackDueAt.Before(at)) {
				at = app.space.ackDueAt
			}
		}
	}
	return at
}
