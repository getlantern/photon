package transport

import (
	"errors"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// A sent ack-eliciting packet already requests a response. Recovery owns further
// probes until another authenticated receive; keepalive cannot extend idle life
// by repeatedly sending into a silent connection.
func (c *Conn) keepAliveDeadline() time.Time {
	if c.config.KeepAlivePeriod == 0 || !c.confirmed || c.closed || c.err != nil || c.Closing() || c.Draining() || c.idle.last.IsZero() || c.idle.sentSinceReceive {
		return time.Time{}
	}
	period := c.config.KeepAlivePeriod
	limit := c.config.Parameters.MaxIdleTimeout
	if c.peerParams != nil && c.peerParams.MaxIdleTimeout != 0 && (limit == 0 || c.peerParams.MaxIdleTimeout < limit) {
		limit = c.peerParams.MaxIdleTimeout
	}
	if limit != 0 {
		period = min(period, milliseconds(limit)/2)
	}
	maxDelay := c.keepAliveACKDelay()
	pto := c.recovery.RTT.PTO(true, c.confirmed, maxDelay, 0)
	applyFloor := true
	if c.multipath != nil {
		_, pto, applyFloor = c.keepAlivePath(maxDelay)
	}
	if applyFloor {
		const maxDuration = time.Duration(1<<63 - 1)
		floor := maxDuration
		if pto <= maxDuration-pto/2 {
			floor = pto + pto/2
		}
		period = max(period, floor)
	}
	base := c.idle.last
	if c.keepAliveAttempt.After(base) {
		base = c.keepAliveAttempt
	}
	return base.Add(period)
}

func (c *Conn) queueKeepAlive(now time.Time) error {
	at := c.keepAliveDeadline()
	if at.IsZero() || now.Before(at) {
		return nil
	}
	// Even a full send queue or lack of a viable path must not leave a due timer
	// spinning. An attempt is not a send and must not update idle activity.
	c.keepAliveAttempt = now
	if c.availableSendBytes() <= 0 {
		return nil
	}
	var err error
	if c.multipath == nil {
		if c.hasQueuedData() {
			return nil
		}
		err = c.enqueue(applicationSpace, wire.PingFrame{}, 1)
	} else {
		selected, _, found := c.keepAlivePath(c.keepAliveACKDelay())
		if !found {
			return nil
		}
		err = c.queueFrameOnPath(selected, wire.PingFrame{})
	}
	if errors.Is(err, ErrSendBuffer) {
		return nil
	}
	return err
}

func (c *Conn) keepAliveACKDelay() time.Duration {
	if c.peerParams != nil {
		return milliseconds(c.peerParams.MaxAckDelay)
	}
	return 25 * time.Millisecond
}

// keepAlivePath chooses a connection-liveness probe route, not a per-path
// keepalive policy. The timer and enqueue use the same eligible-path selection.
// Prefer the smallest base PTO so a slow retained path cannot delay a healthy
// path's probe. Packet output still enforces pacing and congestion control.
func (c *Conn) keepAlivePath(maxDelay time.Duration) (selected uint64, pto time.Duration, found bool) {
	for id, state := range c.multipath.paths {
		app := state.application
		if app == nil || c.multipath.abandoned[id] != nil {
			continue
		}
		if len(state.peer.active[state.peer.current].cid) == 0 || app.localMigration != nil || app.mtuNeedsValidation || (id != 0 && app.validation.state != PathValidated) {
			continue
		}
		// Held traffic on another path must not suppress a healthy empty path.
		queued := false
		for _, item := range app.space.queue {
			if !item.acked {
				queued = true
				break
			}
		}
		if queued {
			continue
		}
		candidate := app.sent.RTT.PTO(true, c.confirmed, maxDelay, 0)
		if !found || candidate < pto || (candidate == pto && id < selected) {
			selected, pto, found = id, candidate, true
		}
	}
	return
}
