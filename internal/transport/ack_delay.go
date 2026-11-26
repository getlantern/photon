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

// bundleACK appends the queued frame f to the packet payload, preceded by the
// ACK the space owes, due or not, so the ACK rides on data instead of taking a
// datagram of its own. It bundles only an ACK that already fits beside f:
// splitting f to make room would leave a small tail needing its own datagram,
// cost a delivery ledger, and happen before the packet is known to be
// sendable. An ACK that does not fit stays owed. bundled reports whether the
// ACK was added, and largest is its largest acknowledged packet number; the
// ACK itself is not returned, as it borrows the receive history's storage.
func (c *Conn) bundleACK(s *numberSpace, space int, early bool, item *sendItem, f wire.Frame, now time.Time) (payload []byte, largest uint64, bundled bool, err error) {
	payload = c.payloadScratch[:0]
	if space != applicationSpace || early || !ackBundleable(s, item, f) {
		payload, err = f.Append(payload)
		return payload, 0, false, err
	}
	ack, err := s.received.ackView(now, uint8(c.config.Parameters.AckDelayExponent))
	if err != nil {
		return nil, 0, false, err
	}
	if payload, err = ack.Append(payload); err != nil {
		return nil, 0, false, err
	}
	ackBytes := len(payload)
	if payload, err = f.Append(payload); err != nil {
		return nil, 0, false, err
	}
	// A 1-RTT short header: first byte, the peer's CID, a 4-byte packet number.
	// The ACK must not hold back a packet that could leave without it, so the
	// bundled size must also pass congestion, pacing and the address budget.
	size := 1 + len(c.peerCID) + 4 + len(payload) + 16
	// The base-path rollback applies only once the handshake is confirmed.
	if size <= bundledSizeLimit(f, c.basePacketPayloadLimit(), size-ackBytes, c.confirmed) && c.canSendBundled(size, wire.InFlightFrame(f), now) {
		return payload, ack.Ranges[0].Largest, true, nil
	}
	payload, err = f.Append(payload[:0])
	return payload, 0, false, err
}

// bundlePathACK is bundleACK for a multipath packet on path ackPath: it
// rebuilds the payload with that path's owed ACK ahead of f when the result
// still fits the path and passes its congestion, pacing and address budget.
// Otherwise the payload is f alone and the ACK stays owed.
func (c *Conn) bundlePathACK(app *pathApplication, ackPath uint64, f wire.Frame, headerLen int, inFlight, probe bool, now time.Time) (payload []byte, largest uint64, bundled bool, err error) {
	ack, err := app.ack(ackPath, now, uint8(c.config.Parameters.AckDelayExponent))
	if err != nil {
		return nil, 0, false, err
	}
	if payload, err = c.pathWireFrame(ack).Append(c.payloadScratch[:0]); err != nil {
		return nil, 0, false, err
	}
	ackBytes := len(payload)
	if payload, err = f.Append(payload); err != nil {
		return nil, 0, false, err
	}
	size := headerLen + len(payload) + 16
	if size <= bundledSizeLimit(f, app.packetPayloadLimit(), size-ackBytes, true) && app.budget.CanSend(size) && app.congestion.CanSend(app.sent.BytesInFlight, size, inFlight, probe) &&
		(!inFlight || probe || app.pacer.DelayFor(now, app.congestion, app.sent.RTT.Smoothed, size) <= 0) {
		return payload, ack.Ranges[0].Largest, true, nil
	}
	payload, err = f.Append(c.payloadScratch[:0])
	return payload, 0, false, err
}

// bundledSizeLimit caps a packet grown by a bundled ACK, where alone is the
// packet's size without the ACK. A packet within the minimum QUIC MTU, which
// every path supports, must stay within it unless its frame is STREAM data
// that a write rejected as too large can requeue (rollback, see
// HandleWriteError). A rejected DATAGRAM is dropped and any other frame's
// rejection removes the connection, so for those the ACK must not be what
// makes the packet too large. A packet already past the minimum MTU without
// the ACK may grow to limit, since its rejection does not depend on the ACK.
func bundledSizeLimit(f wire.Frame, limit, alone int, rollback bool) int {
	if alone > minimumQUICMTU {
		return limit
	}
	if _, stream := f.(wire.StreamFrame); stream && rollback {
		return limit
	}
	return min(limit, minimumQUICMTU)
}

// ackBundleable reports whether the queued frame f may carry the ACK the
// space owes, due or not, ahead of it. PATH_CHALLENGE and PATH_RESPONSE stay
// alone, since they are padded and sent for path validation.
func ackBundleable(s *numberSpace, item *sendItem, f wire.Frame) bool {
	if item == nil || !(s.ackPending || s.unackedEliciting > 0) || !wire.AckEliciting(f) {
		return false
	}
	switch f.(type) {
	case wire.PathChallengeFrame, wire.PathResponseFrame:
		return false
	}
	_, ok := s.received.Largest()
	return ok
}

// restoreBundledACK owes again an ACK whose packet a write rejected, at once
// rather than at its old deadline: the rejection is rare, and an early ACK is
// always allowed. If that ACK is what marked an ACK sent in this key phase
// (ackSent was false before it), it unmarks it; a key phase that has since
// moved on, identified by the key generation, keeps its own state.
func (c *Conn) restoreBundledACK(s *numberSpace, ackSent bool, generation uint64) {
	s.ackPending = true
	if !ackSent && c.applicationKeys.generation == generation {
		c.applicationKeys.ackSent = false
	}
}

// canSendBundled reports whether a packet of size, grown by a bundled ACK,
// still passes the congestion window, pacing and the anti-amplification
// budget, which NextDatagram checks after the payload is built.
func (c *Conn) canSendBundled(size int, inFlight bool, now time.Time) bool {
	if !c.congestion.CanSend(c.recovery.BytesInFlight, size, inFlight, false) || !c.budget.CanSend(size) {
		return false
	}
	return !inFlight || c.pacer.DelayFor(now, c.congestion, c.recovery.RTT.Smoothed, size) <= 0
}
