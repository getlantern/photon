package transport

import (
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// RFC 9000 section 13.2.1 lets a receiver hold the ACK for an ack-eliciting
// application packet for up to its advertised max_ack_delay, but it SHOULD
// acknowledge at once after every second one; acknowledging every packet
// instead roughly doubles the datagrams a bulk transfer sends in the reverse
// direction. A packet that arrives out of order (a duplicate always does, so
// its ACK may replace a lost one) or marked ECN-CE is acknowledged at once, as
// are Initial and Handshake packets. A max_ack_delay of zero makes every
// deadline already due.
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

// expireACKDelays makes every due delayed application ACK pending, so a past
// deadline leaves RecoveryDeadline even when no packet can be sent yet.
func (c *Conn) expireACKDelays(now time.Time) {
	c.spaces[applicationSpace].expireACKDelay(now)
	if c.multipath != nil {
		for _, state := range c.multipath.paths {
			if app := state.application; app != nil {
				app.space.expireACKDelay(now)
			}
		}
	}
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

// bundleACK appends the queued frame *f to the packet payload, preceded by the
// ACK the space owes, due or not, so the ACK rides on data instead of taking a
// datagram of its own. It returns the bundled ACK, or nil when none was added.
// A stream frame that no longer fits beside the ACK is split, which updates
// *item and *f. Any other frame that does not fit, or a stream frame whose
// split fails, is sent without the ACK.
func (c *Conn) bundleACK(s *numberSpace, space int, early bool, item **sendItem, f *wire.Frame, now time.Time) ([]byte, *wire.AckFrame, error) {
	payload := c.payloadScratch[:0]
	owed := s.ackPending || s.unackedEliciting > 0
	if *item == nil || space != applicationSpace || early || !owed || !wire.AckEliciting(*f) {
		payload, err := (*f).Append(payload)
		return payload, nil, err
	}
	switch (*f).(type) {
	case wire.PathChallengeFrame, wire.PathResponseFrame:
		payload, err := (*f).Append(payload)
		return payload, nil, err
	}
	ack, err := s.received.ackView(now, uint8(c.config.Parameters.AckDelayExponent))
	if err != nil {
		return nil, nil, err
	}
	if payload, err = ack.Append(payload); err != nil {
		return nil, nil, err
	}
	ackLen := len(payload)
	if payload, err = (*f).Append(payload); err != nil {
		return nil, nil, err
	}
	header, err := c.makeHeader(space, s.nextPN, len(payload)+16)
	if err != nil {
		return nil, nil, err
	}
	excess := len(header) + len(payload) + 16 - c.basePacketPayloadLimit()
	if excess <= 0 {
		return payload, &ack, nil
	}
	if frame, ok := (*f).(wire.StreamFrame); ok && len(frame.Data) > excess {
		// The split is only to make room for an optional ACK: if it fails
		// (its delivery or send-buffer bounds), which leaves the queue as it
		// was, send the frame alone and keep the ACK owed. A peer that grows
		// our ACK with gaps must not be able to fail the connection this way.
		if err := c.splitQueuedStream(s, 0, len(frame.Data)-excess); err != nil {
			payload, err = (*f).Append(payload[:0])
			return payload, nil, err
		}
		*item, *f = s.queue[0], s.queue[0].frame
		if payload, err = (*f).Append(payload[:ackLen]); err != nil {
			return nil, nil, err
		}
		return payload, &ack, nil
	}
	payload, err = (*f).Append(payload[:0])
	return payload, nil, err
}
