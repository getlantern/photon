package transport

import (
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func (c *Conn) armTimer(now time.Time) {
	if c.err != nil || c.closed {
		return
	}
	if c.multipath != nil && c.confirmed {
		for id := range c.multipath.paths {
			c.armPathRecovery(id, now)
		}
		return
	}
	maxDelay := 25 * time.Millisecond
	if c.peerParams != nil {
		maxDelay = time.Duration(c.peerParams.MaxAckDelay) * time.Millisecond
	}
	// Only an unvalidated server path has an amplification ceiling. Avoid
	// constructing a probe header/payload merely to size it on every ACK/data
	// timer update after validation (or for a client).
	blocked := false
	if c.config.Server && !c.budget.Validated() {
		// The smallest legal probe determines whether credit permits progress.
		space := initialSpace
		if c.writeKeys(handshakeSpace) != nil {
			space = handshakeSpace
		}
		if c.confirmed {
			space = applicationSpace
		}
		blocked = !c.budget.CanSend(c.probeSize(space, wire.PingFrame{}))
	}
	c.timer.Arm(c.recovery, now, recovery.TimerState{
		PeerValidated:        c.config.Server || c.handshakeACK || c.confirmed,
		HandshakeKeys:        c.writeKeys(handshakeSpace) != nil,
		Confirmed:            c.confirmed,
		AmplificationBlocked: blocked,
		MaxACKDelay:          maxDelay,
	})
}

func (c *Conn) probeSize(space int, frame wire.Frame) int {
	if space == initialSpace {
		return 1200
	}
	payload, err := frame.Append(nil)
	if err != nil {
		return 65536
	}
	header, err := c.makeHeader(space, c.spaces[space].nextPN, len(payload)+16)
	if err != nil {
		return 65536
	}
	return len(header) + len(payload) + 16
}

// RecoveryDeadline returns the draining deadline, or the earliest loss/PTO,
// idle, keepalive, path validation/retention, delayed application ACK, or
// old-key retirement deadline without moving it. While a foreign write is
// outstanding, only connection lifetime and key-retirement deadlines remain
// eligible: output cannot advance recovery, validation or an ACK until the
// socket result has been consumed. A zero value means no eligible timer is
// armed.
func (c *Conn) RecoveryDeadline() time.Time {
	if c.Closing() {
		return c.closing.until
	}
	if c.Draining() {
		return c.drainUntil
	}
	if c.err != nil || c.closed {
		return time.Time{}
	}
	var at, pacing, path time.Time
	if c.pendingWrite == nil {
		at = c.timer.Deadline()
		pacing, path = c.pacingAt, c.pathValidationDeadline()
	}
	if c.pendingWrite == nil && c.multipath != nil && c.confirmed {
		at = c.multipathRecoveryDeadline()
		pacing = time.Time{} // Included separately for every application path.
		path = c.multipathPathsDeadline()
	}
	if handshake := c.HandshakeDeadline(); !handshake.IsZero() && (at.IsZero() || handshake.Before(at)) {
		at = handshake
	}
	if expiry := c.earlyReadExpiry; !expiry.IsZero() && (at.IsZero() || expiry.Before(at)) {
		at = expiry
	}
	if expiry := c.applicationKeys.previousExpiry; !expiry.IsZero() && (at.IsZero() || expiry.Before(at)) {
		at = expiry
	}
	if keepalive := c.keepAliveDeadline(); c.pendingWrite == nil && !keepalive.IsZero() && (at.IsZero() || keepalive.Before(at)) {
		at = keepalive
	}
	if idle := c.IdleDeadline(); !idle.IsZero() && (at.IsZero() || idle.Before(at)) {
		at = idle
	}
	if !pacing.IsZero() && (at.IsZero() || pacing.Before(at)) {
		at = pacing
	}
	if !path.IsZero() && (at.IsZero() || path.Before(at)) {
		at = path
	}
	if ack := c.ackDeadline(); c.pendingWrite == nil && !ack.IsZero() && (at.IsZero() || ack.Before(at)) {
		at = ack
	}
	return at
}

// HandleTimeout processes a due recovery timer. The caller must drain
// NextDatagram after this call (nextMultipathPacket for an attached multipath
// owner); sends rearm PTO using actual transmission time.
func (c *Conn) HandleTimeout(now time.Time) error {
	defer c.publishStatistics()
	if c.Closing() {
		if !now.Before(c.closing.until) {
			c.Close()
		}
		return nil
	}
	if c.Draining() {
		if !now.Before(c.drainUntil) {
			c.closed = true
			if c.pendingWrite != nil {
				c.pendingWrite.release()
			}
			c.drainUntil = time.Time{}
			c.peerIDs = nil
			c.localIDs = nil
			c.multipath = nil
		}
		return nil
	}
	if err := c.check(); err != nil {
		return err
	}
	if err := c.checkHandshakeTimeout(now); err != nil {
		return err
	}

	if deadline := c.IdleDeadline(); !deadline.IsZero() && !now.Before(deadline) {
		err := c.fail(ErrIdleTimeout)
		c.Close()
		return err
	}
	c.expirePreviousKeys(now)
	c.expireEarlyKeys(now)
	c.expireACKDelays(now)
	if c.pendingWrite != nil {
		return nil
	}
	if err := c.queueKeepAlive(now); err != nil {
		return err
	}
	if c.multipath != nil && c.confirmed {
		// Resolve validation failure and retention before requesting probes,
		// so a path stopped at this instant cannot acquire another PTO probe.
		if err := c.expireMultipathPaths(now); err != nil {
			return err
		}
		return c.expireMultipathRecovery(now)
	}
	if err := c.expirePathValidation(now); err != nil {
		return err
	}
	event, ok := c.timer.Expire(c.recovery, now)
	if !ok {
		return nil
	}
	space := int(event.Space)
	if !event.Probe {
		c.onCongestionLoss(event.Lost, now)
		c.queueLost(space, event.Lost)
		c.armTimer(now)
		return nil
	}
	c.probeAllowance[space] = true
	s := &c.spaces[space]
	// Prefer queued data, then the oldest outstanding item, then PING. Only
	// choose data that fits the current path credit; PTO cannot bypass it.
	var chosen *sendItem
	for _, item := range s.queue {
		if !item.acked && c.budget.CanSend(c.probeSize(space, item.frame)) {
			chosen = item
			break
		}
	}
	if chosen == nil {
		oldest := ^uint64(0)
		for pn, item := range s.sent {
			if pn < oldest {
				visitSendLeaves(item, func(part *sendItem) bool {
					if !part.queued && c.budget.CanSend(c.probeSize(space, part.frame)) {
						chosen, oldest = part, pn
						return true
					}
					return false
				})
			}
		}
	}
	if chosen == nil {
		if err := c.enqueue(space, wire.PingFrame{}, 1); err != nil {
			return err
		}
		chosen = s.queue[len(s.queue)-1]
	}
	// Move the probe ahead of data blocked by amplification credit.
	queue := s.queue[:0]
	for _, item := range s.queue {
		if item != chosen {
			queue = append(queue, item)
		}
	}
	s.queue = append(queue, nil)
	copy(s.queue[1:], s.queue[:len(s.queue)-1])
	s.queue[0] = chosen
	chosen.queued = true
	return nil
}
