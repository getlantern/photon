package transport

import (
	"time"
)

type idleActivity struct {
	last             time.Time
	sentSinceReceive bool
}

func (i *idleActivity) received(now time.Time) {
	if i.last.IsZero() || now.After(i.last) {
		i.last = now
	}
	i.sentSinceReceive = false
}
func (i *idleActivity) sent(now time.Time, eliciting bool) {
	if i.last.IsZero() {
		i.last = now
	}
	if eliciting && !i.sentSinceReceive {
		if now.After(i.last) {
			i.last = now
		}
		i.sentSinceReceive = true
	}
}
func milliseconds(value uint64) time.Duration {
	if value > uint64(time.Duration(1<<63-1)/time.Millisecond) {
		return time.Duration(1<<63 - 1)
	}
	return time.Duration(value) * time.Millisecond
}
func triple(duration time.Duration) time.Duration {
	if duration > time.Duration(1<<63-1)/3 {
		return time.Duration(1<<63 - 1)
	}
	return 3 * duration
}

// IdleDeadline applies the minimum nonzero negotiated max_idle_timeout and
// RFC 9000's three-base-PTO floor, using the largest retained application-path
// PTO after multipath activation. Reads never move the activity timestamp.
// Before peer parameters arrive, the local advertised timeout applies.
func (c *Conn) IdleDeadline() time.Time {
	if c.err != nil || c.closed || c.idle.last.IsZero() {
		return time.Time{}
	}
	limit := c.config.Parameters.MaxIdleTimeout
	maxDelay := 25 * time.Millisecond
	if c.peerParams != nil {
		peer := c.peerParams.MaxIdleTimeout
		if peer != 0 && (limit == 0 || peer < limit) {
			limit = peer
		}
		maxDelay = time.Duration(c.peerParams.MaxAckDelay) * time.Millisecond
	}
	if limit == 0 {
		return time.Time{}
	}
	duration := max(milliseconds(limit), triple(c.retentionPTO(true, maxDelay)))
	return c.idle.last.Add(duration)
}

// Connection-level event revisions distinguish activity even when clocks match.
// Local write rejection must never roll back an intervening send or receive.
func (c *Conn) receivedIdleActivity(now time.Time) {
	c.idleRevision++
	c.idle.received(now)
}
func (c *Conn) sentIdleActivity(now time.Time, eliciting bool) {
	c.idleRevision++
	c.idle.sent(now, eliciting)
}
func (c *Conn) restoreIdleActivity(before idleActivity, revision uint64) {
	if c.idleRevision == revision {
		c.idle = before
		c.idleRevision++
	}
}
