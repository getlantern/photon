package transport

import (
	"crypto/rand"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// PathValidationState describes the most recent explicit fixed-path check.
// Validated includes successful transmission of a 1200-byte challenge.
type PathValidationState uint8

const (
	PathUnchecked PathValidationState = iota
	PathValidating
	PathValidated
	PathValidationFailed
)

type pathProbe struct {
	token    [8]byte
	expanded bool
}
type pathValidation struct {
	state         PathValidationState
	probes        []pathProbe
	pending       bool
	next, expires time.Time
	interval      time.Duration
}

func (c *Conn) PathValidationState() PathValidationState { return c.validation.state }

// ValidatePath checks the current adapter-associated path. It does not change
// addresses or connection IDs. Repeated calls while probing are idempotent.
func (c *Conn) ValidatePath() error {
	if err := c.check(); err != nil {
		return err
	}
	if !c.confirmed {
		return ErrHandshakeUnconfirmed
	}
	if c.validation.state == PathValidating {
		return nil
	}
	delay := 25 * time.Millisecond
	if c.peerParams != nil {
		delay = time.Duration(c.peerParams.MaxAckDelay) * time.Millisecond
	}
	c.validation = pathValidation{state: PathValidating, interval: c.recovery.RTT.PTO(true, true, delay, 0)}
	return c.queuePathChallenge()
}
func (c *Conn) queuePathChallenge() error {
	var token [8]byte
	if _, err := rand.Read(token[:]); err != nil {
		return c.fail(err)
	}
	if c.baseMTUNeedsValidation {
		c.baseMTUChallenge = &wire.PathChallengeFrame{Data: token}
	} else if err := c.enqueue(applicationSpace, wire.PathChallengeFrame{Data: token}, 32); err != nil {
		return err
	}
	c.validation.pending = true
	c.validation.next = time.Time{}
	return nil
}
func (c *Conn) sentPathChallenge(f wire.PathChallengeFrame, size int, now time.Time) {
	c.validation.recordSent(f, size, now)
}

func (v *pathValidation) recordSent(f wire.PathChallengeFrame, size int, now time.Time) {
	if v.state != PathValidating || len(v.probes) >= 3 {
		return
	}
	v.probes = append(v.probes, pathProbe{token: f.Data, expanded: size >= 1200})
	v.pending = false
	if v.expires.IsZero() {
		v.expires = now.Add(triple(v.interval))
	}
	if len(v.probes) < 3 {
		v.next = now.Add(v.interval)
	}
}
func (c *Conn) pathValidationDeadline() time.Time {
	v := &c.validation
	if v.state != PathValidating {
		return time.Time{}
	}
	if !v.next.IsZero() && (v.expires.IsZero() || v.next.Before(v.expires)) {
		return v.next
	}
	return v.expires
}
func (c *Conn) retirePathChallenges() {
	c.baseMTUChallenge = nil
	c.retireFrames(func(f wire.Frame) bool { _, ok := f.(wire.PathChallengeFrame); return ok })
}
func (c *Conn) expirePathValidation(now time.Time) error {
	v := &c.validation
	if v.state != PathValidating {
		return nil
	}
	if !v.expires.IsZero() && !now.Before(v.expires) {
		c.retirePathChallenges()
		c.validation = pathValidation{state: PathValidationFailed}
		return nil
	}
	if !v.pending && !v.next.IsZero() && !now.Before(v.next) {
		return c.queuePathChallenge()
	}
	return nil
}
func (c *Conn) handlePathResponse(f wire.PathResponseFrame, now time.Time) error {
	if err := c.expirePathValidation(now); err != nil {
		return err
	}
	if c.validation.state != PathValidating {
		return nil
	}
	for _, p := range c.validation.probes {
		if p.token != f.Data {
			continue
		}
		c.budget.Validate()
		c.retirePathChallenges()
		if p.expanded {
			c.validation = pathValidation{state: PathValidated}
			return nil
		}
		// An unexpanded probe validates the address but not the minimum path MTU.
		interval := c.validation.interval
		c.validation = pathValidation{state: PathValidating, interval: interval}
		return c.queuePathChallenge()
	}
	return nil
}

// matchResponse recognizes only a challenge actually sent in this path context.
func (v *pathValidation) matchResponse(f wire.PathResponseFrame, now time.Time) (bool, bool) {
	if v.state != PathValidating || (!v.expires.IsZero() && !now.Before(v.expires)) {
		return false, false
	}
	for _, p := range v.probes {
		if p.token == f.Data {
			return true, p.expanded
		}
	}
	return false, false
}
