package transport

import (
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// survivingPath selects a prepared context other than the path being stopped.
// Validation and credit are still enforced when its queued packets are sent.
func (c *Conn) survivingPath(exclude uint64) (uint64, bool) {
	var selected uint64
	found := false
	for id, state := range c.multipath.paths {
		if id == exclude || state.application == nil || c.multipath.abandoned[id] != nil || len(state.peer.active[state.peer.current].cid) == 0 {
			continue
		}
		if !found || id < selected {
			selected, found = id, true
		}
	}
	return selected, found
}

// evacuatePath keeps old sent-packet references for late ACKs while moving
// logical items once. Path-affine probes and obsolete CID advertisements are
// discarded; queued application data keeps its existing memory charge.
func (c *Conn) evacuatePath(id uint64) error {
	state := c.multipath.paths[id]
	if state == nil || state.application == nil {
		return nil
	}
	dest, ok := c.survivingPath(id)
	if !ok {
		return &TransportError{Code: 0x10, Reason: "no viable path after abandonment"}
	}
	c.evacuatePathTo(id, dest)
	return nil
}

// evacuatePathTo requires live source/destination contexts, selected by the owner.
func (c *Conn) evacuatePathTo(id, dest uint64) {
	app := c.multipath.paths[id].application
	app.migration = nil
	app.localMigration = nil
	app.migrationResponse = nil
	app.timer.DiscardKeys()
	app.probePending = false
	app.pacingAt = time.Time{}
	destination := c.multipath.paths[dest].application.space
	discard := func(item *sendItem) bool {
		switch f := item.frame.(type) {
		case wire.PathChallengeFrame, wire.PathResponseFrame:
			return true
		case wire.NewConnectionIDFrame:
			return c.multipath.abandoned[0] != nil
		case wire.PathNewConnectionIDFrame:
			return c.multipath.abandoned[f.PathID] != nil
		}
		return false
	}
	move := func(item *sendItem) {
		if item.acked {
			return
		}
		if discard(item) {
			item.acked = true
			c.queuedBytes -= item.bytes
			return
		}
		if !item.queued {
			item.queued = true
			destination.queue = append(destination.queue, item)
		}
	}
	for _, item := range app.space.queue {
		item.queued = false
		visitSendLeaves(item, func(part *sendItem) bool { move(part); return false })
	}
	clear(app.space.queue)
	app.space.queue = nil
	if app.evacuated {
		// A copy already sent on a survivor is unqueued but still outstanding
		// there; only that path's loss detection or PTO may queue it again.
		return
	}
	app.evacuated = true
	for _, item := range app.space.sent {
		visitSendLeaves(item, func(part *sendItem) bool { move(part); return false })
	}
}

func (c *Conn) abandonFailedPath(id uint64) error {
	if c.multipath.abandoned[id] != nil {
		return nil
	}
	dest, ok := c.survivingPath(id)
	if !ok {
		return c.fail(&TransportError{Code: 0x10, Reason: "path validation failed without a surviving path"})
	}
	if err := c.queueFrameOnPath(dest, wire.PathAbandonFrame{PathID: id}); err != nil {
		return c.fail(err)
	}
	if _, err := c.multipath.beginAbandon(id); err != nil {
		return c.fail(err)
	}
	if err := c.evacuatePath(id); err != nil {
		return c.fail(err)
	}
	return nil
}

// expireMultipathPaths services validation and retention. The network owner
// combines this deadline with recovery, connection idle, and key retirement.
func (c *Conn) expireMultipathPaths(now time.Time) error {
	if err := c.check(); err != nil {
		return err
	}
	if c.multipath == nil || !c.confirmed {
		return ErrHandshakeUnconfirmed
	}
	for id, state := range c.multipath.paths {
		if state.application == nil || c.multipath.abandoned[id] != nil {
			continue
		}
		if state.application.migration != nil {
			state.application.migration.expire(now)
		}
		if err := c.expireValidationOnPath(id, now); err != nil {
			return c.fail(err)
		}
		if state.application.validation.state == PathValidationFailed {
			if err := c.abandonFailedPath(id); err != nil {
				return err
			}
		}
	}
	for id := range c.multipath.paths {
		if c.multipath.abandoned[id] != nil {
			if err := c.evacuatePath(id); err != nil {
				return c.fail(err)
			}
		}
	}
	// All undelivered items are now queued or outstanding on a surviving path;
	// late ACKs could already have completed them. Disposal only removes the
	// old references.
	c.multipath.collectAbandoned(now)
	return nil
}

func (c *Conn) multipathPathsDeadline() time.Time {
	var at time.Time
	add := func(t time.Time) {
		if !t.IsZero() && (at.IsZero() || t.Before(at)) {
			at = t
		}
	}
	if c.multipath == nil {
		return at
	}
	for id, state := range c.multipath.paths {
		if a := c.multipath.abandoned[id]; a != nil {
			if a.sent && a.received {
				add(a.retainUntil)
			}
			continue
		}
		if state.application == nil {
			continue
		}
		if m := state.application.migration; m != nil && m.candidate != nil && m.candidate.validation.state == PathValidating {
			add(m.candidate.validation.expires)
			add(m.candidate.pacingAt)
			v := &m.candidate.validation
			if !v.pending && len(v.probes) < 3 {
				add(v.next)
			}
		}
		if candidate := state.application.localMigration; candidate != nil {
			if candidate.validation.state == PathValidating {
				add(candidate.validation.expires)
				add(candidate.pacingAt)
				if !candidate.validation.pending && len(candidate.validation.probes) < 3 {
					add(candidate.validation.next)
				}
			} else if candidate.validation.state == PathValidated && candidate.responsePending != nil {
				add(candidate.pacingAt)
			}
		}
		v := state.application.validation
		if v.state == PathValidating {
			add(v.expires)
			if !v.pending && !state.application.routeBlocked {
				add(v.next)
			}
		}
	}
	return at
}
