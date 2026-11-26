package transport

import (
	"github.com/getlantern/photon/internal/recovery"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func (c *Conn) armPathRecovery(id uint64, now time.Time) {
	state := c.multipath.paths[id]
	if state == nil || state.application == nil || state.application.routeBlocked || c.multipath.abandoned[id] != nil {
		return
	}
	peer := state.peer.active[state.peer.current]
	blocked := len(peer.cid) == 0 || !state.application.budget.CanSend(1+len(peer.cid)+4+16+1)
	state.application.arm(now, time.Duration(c.peerParams.MaxAckDelay)*time.Millisecond, blocked)
}

// multipathRecoveryDeadline observes prepared paths without moving their timers.
// Connection idle/key retirement and path validation have separate deadlines.
func (c *Conn) multipathRecoveryDeadline() time.Time {
	var at time.Time
	if c.multipath == nil {
		return at
	}
	for id, state := range c.multipath.paths {
		if state.application == nil || state.application.routeBlocked || c.multipath.abandoned[id] != nil {
			continue
		}
		app := state.application
		for _, candidate := range []time.Time{app.timer.Deadline(), app.pacingAt, app.mtuProbeDeadline(), app.mtuRaiseDeadline()} {
			if !candidate.IsZero() && (at.IsZero() || candidate.Before(at)) {
				at = candidate
			}
		}
	}
	return at
}

// expireMultipathRecovery consumes due per-path loss/PTO timers. The owner
// drains nextMultipathPacket afterward; actual probe sends restart PTO.
// It must refresh amplification credit before calling this method.
func (c *Conn) expireMultipathRecovery(now time.Time) error {
	if err := c.check(); err != nil {
		return err
	}
	if c.multipath == nil || !c.confirmed {
		return ErrHandshakeUnconfirmed
	}
	maxDelay := time.Duration(c.peerParams.MaxAckDelay) * time.Millisecond
	for id, state := range c.multipath.paths {
		if state.application == nil || state.application.routeBlocked || c.multipath.abandoned[id] != nil {
			continue
		}
		app := state.application
		app.expireMTUProbe(now)
		app.expireMTURaise(now)
		if !app.pacingAt.IsZero() && !now.Before(app.pacingAt) {
			app.pacingAt = time.Time{}
		}
		c.armPathRecovery(id, now)
		event, due := app.timer.Expire(app.sent, now)
		if !due {
			continue
		}
		if !event.Probe {
			c.loss.lost(event.Lost)
			app.noteMTULoss(event.Lost)
			app.observeMTUBlackhole(nil, event.Lost, now)
			app.updateMTURaise(now)
			app.congestion.OnLoss(recovery.LossEvent{Lost: event.Lost, TotalLost: app.sent.LostBytes(), Flight: app.sent.BytesInFlight, Now: now})
			if len(event.Lost) > 0 && app.sent.PersistentCongestion(maxDelay) {
				app.congestion.OnPersistentCongestion()
				app.sent.RTT.OnPersistentCongestion()
			}
			queueLostItems(app.space, app.space, event.Lost)
			c.armPathRecovery(id, now)
			continue
		}
		if err := c.queuePathProbe(id); err != nil {
			return c.fail(err)
		}
		app.probePending = true
	}
	return nil
}

func (c *Conn) queuePathProbe(id uint64) error {
	state := c.multipath.paths[id]
	app := state.application
	if app.hasPathResponse() {
		return nil
	}
	if app.mtuNeedsValidation && app.validation.state != PathValidationFailed {
		if app.validation.state != PathValidating {
			return c.validateMultipathPath(id)
		}
		if app.mtuChallenge == nil && len(app.validation.probes) < 3 {
			return c.queueChallengeOnPath(id)
		}
		return nil
	}
	eligible := func(item *sendItem) bool {
		if item.acked || c.blocksCIDAdvertisement(item.frame) || !wire.AckEliciting(item.frame) {
			return false
		}
		if (id != 0 || app.mtuNeedsValidation) && app.validation.state != PathValidated {
			switch item.frame.(type) {
			case wire.StreamFrame, wire.DatagramFrame:
				return false
			}
		}
		b, err := item.frame.Append(nil)
		size := 1 + len(state.peer.active[state.peer.current].cid) + 4 + len(b) + 16
		return err == nil && size <= 1200 && app.budget.CanSend(size)
	}
	var chosen *sendItem
	for _, item := range app.space.queue {
		if eligible(item) {
			chosen = item
			break
		}
	}
	if chosen == nil {
		oldest := ^uint64(0)
		for pn, item := range app.space.sent {
			if pn < oldest {
				visitSendLeaves(item, func(part *sendItem) bool {
					if !part.queued && eligible(part) {
						chosen, oldest = part, pn
						return true
					}
					return false
				})
			}
		}
	}
	if chosen == nil {
		if err := c.queueFrameOnPath(id, wire.PingFrame{}); err != nil {
			return err
		}
		chosen = app.space.queue[len(app.space.queue)-1]
	}
	queue := app.space.queue[:0]
	for _, item := range app.space.queue {
		if item != chosen {
			queue = append(queue, item)
		}
	}
	queue = append(queue, nil)
	copy(queue[1:], queue[:len(queue)-1])
	queue[0] = chosen
	chosen.queued = true
	app.space.queue = queue
	return nil
}
