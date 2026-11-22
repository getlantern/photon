package transport

import (
	"crypto/rand"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// validateMultipathPath starts validation of an already bound network context.
// Address binding and client-only path initiation must be enforced by the owner.
func (c *Conn) validateMultipathPath(id uint64) error {
	if err := c.check(); err != nil {
		return err
	}
	if c.multipath == nil || !c.confirmed {
		return ErrHandshakeUnconfirmed
	}
	state := c.multipath.paths[id]
	if state == nil || state.application == nil || c.multipath.abandoned[id] != nil {
		return errNoPathForControl
	}
	app := state.application
	if app.validation.state == PathValidating {
		return nil
	}
	delay := time.Duration(c.peerParams.MaxAckDelay) * time.Millisecond
	old := *app.validation
	*app.validation = pathValidation{state: PathValidating, interval: app.sent.RTT.PTO(true, true, delay, 0)}
	if err := c.queueChallengeOnPath(id); err != nil {
		*app.validation = old
		return err
	}
	return nil
}

func (c *Conn) queueChallengeOnPath(id uint64) error {
	var token [8]byte
	if _, err := rand.Read(token[:]); err != nil {
		return err
	}
	app := c.multipath.paths[id].application
	if app.mtuNeedsValidation {
		app.mtuChallenge = &wire.PathChallengeFrame{Data: token}
	} else if err := c.queueFrameOnPath(id, wire.PathChallengeFrame{Data: token}); err != nil {
		return err
	}
	v := app.validation
	v.pending = true
	v.next = time.Time{}
	return nil
}

func (c *Conn) retireChallengesOnPath(id uint64) {
	app := c.multipath.paths[id].application
	app.mtuChallenge = nil
	for _, item := range app.space.queue {
		if _, ok := item.frame.(wire.PathChallengeFrame); ok && !item.acked {
			item.acked = true
			c.queuedBytes -= item.bytes
		}
	}
}

func (c *Conn) expireValidationOnPath(id uint64, now time.Time) error {
	state := c.multipath.paths[id]
	if state == nil || state.application == nil || c.multipath.abandoned[id] != nil {
		return nil
	}
	v := state.application.validation
	if v.state != PathValidating {
		return nil
	}
	if !v.expires.IsZero() && !now.Before(v.expires) {
		c.retireChallengesOnPath(id)
		*v = pathValidation{state: PathValidationFailed}
		return nil // Owner must abandon a newly initiated path after failure.
	}
	if !v.pending && !v.next.IsZero() && !now.Before(v.next) {
		return c.queueChallengeOnPath(id)
	}
	return nil
}

func (c *Conn) handlePathResponseOnPath(id uint64, f wire.PathResponseFrame, now time.Time) error {
	if err := c.expireValidationOnPath(id, now); err != nil {
		return err
	}
	state := c.multipath.paths[id]
	if state == nil || state.application == nil || c.multipath.abandoned[id] != nil {
		return nil
	}
	app := state.application
	matched, expanded := app.validation.matchResponse(f, now)
	if !matched {
		return nil
	}
	app.budget.Validate()
	c.retireChallengesOnPath(id)
	if expanded {
		app.mtuNeedsValidation = false
		*app.validation = pathValidation{state: PathValidated}
		return nil
	}
	interval := app.validation.interval
	*app.validation = pathValidation{state: PathValidating, interval: interval}
	return c.queueChallengeOnPath(id) // Validate the minimum packet size as well.
}
