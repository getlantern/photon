package transport

import (
	"crypto/rand"
	"errors"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// migrationCandidate is separate from the established path's addresses, credit,
// packet numbers and recovery. Remote candidates require authenticated input;
// locally initiated candidates require client policy admission. Neither alone
// authorizes address switching.
// The owner must bound the number of concurrent candidates and preserve the
// previous validated binding until migration commits or the candidate expires.
type migrationCandidate struct {
	failure            error // local probe failure, retained for the path controller
	requiresNomination bool  // Peer probing alone does not authorize rebinding.
	explicitSwitch     bool  // Probe without nominating or automatically switching the binding.
	preferred          bool  // Authenticated preferred-address destination authorization.
	committed          bool
	freshCID           bool
	peerSequence       uint64
	hasPeerSequence    bool
	pacingAt           time.Time
	tuple              pathTuple
	budget             Amplification
	validation         pathValidation
	pending            wire.PathChallengeFrame
	responsePending    *wire.PathResponseFrame
	responseTail       pathResponseQueue
}

func newMigrationCandidate(tuple pathTuple, authenticatedBytes int, now time.Time, pto time.Duration) (*migrationCandidate, error) {
	if authenticatedBytes < 1 {
		return nil, errors.New("quic: invalid migration candidate credit")
	}
	c, err := newUncreditedMigrationCandidate(tuple, now, pto)
	if err != nil {
		return nil, err
	}
	if err := c.budget.Received(authenticatedBytes); err != nil {
		return nil, err
	}
	return c, nil
}

func newUncreditedMigrationCandidate(tuple pathTuple, now time.Time, pto time.Duration) (*migrationCandidate, error) {
	canonical, err := canonicalPathTuple(tuple.local, tuple.remote)
	if err != nil {
		return nil, err
	}
	if now.IsZero() || pto <= 0 || pto > time.Duration(1<<63-1)/3 {
		return nil, errors.New("quic: invalid migration candidate")
	}
	return &migrationCandidate{tuple: canonical, validation: pathValidation{state: PathValidating, interval: pto, expires: now.Add(3 * pto)}}, nil
}

// expire applies an absolute deadline, including when amplification credit
// prevents the first probe. New input/retries must not extend that deadline.
func (c *migrationCandidate) expire(now time.Time) bool {
	if c.validation.state == PathValidating && !now.Before(c.validation.expires) {
		c.validation.state = PathValidationFailed
		c.validation.probes = nil
		c.validation.pending = false
		c.pending = wire.PathChallengeFrame{}
		c.responsePending = nil
		c.responseTail = pathResponseQueue{}
	}
	return c.validation.state == PathValidationFailed
}

func (c *migrationCandidate) credit(tuple pathTuple, bytes int, now time.Time) error {
	canonical, err := canonicalPathTuple(tuple.local, tuple.remote)
	if err != nil || canonical != c.tuple {
		return errPathBinding
	}
	if c.expire(now) {
		return errors.New("quic: migration candidate expired")
	}
	return c.budget.Received(bytes)
}

// challenge prepares a stable token until output commits. Merely requesting a
// frame does not consume credit or make its token eligible for PATH_RESPONSE.
func (c *migrationCandidate) challenge(now time.Time) (wire.PathChallengeFrame, bool, error) {
	v := &c.validation
	if c.expire(now) || v.state != PathValidating {
		return wire.PathChallengeFrame{}, false, nil
	}
	if v.pending {
		return c.pending, true, nil
	}
	if len(v.probes) >= 3 || (!v.next.IsZero() && now.Before(v.next)) {
		return wire.PathChallengeFrame{}, false, nil
	}
	var token [8]byte
	if _, err := rand.Read(token[:]); err != nil {
		return wire.PathChallengeFrame{}, false, err
	}
	c.pending = wire.PathChallengeFrame{Data: token}
	v.pending = true
	return c.pending, true, nil
}

// sent commits a complete candidate datagram after all packet-output gates.
// RFC 9000 requires minimum-size expansion unless amplification prevents it.
func (c *migrationCandidate) sent(frame wire.PathChallengeFrame, bytes int, now time.Time) error {
	v := &c.validation
	if c.expire(now) || v.state != PathValidating || !v.pending || frame != c.pending || bytes < 9 {
		return errors.New("quic: invalid migration probe commit")
	}
	if bytes < 1200 && c.budget.CanSend(1200) {
		return errors.New("quic: migration probe requires padding")
	}
	if err := c.budget.Sent(bytes); err != nil {
		return err
	}
	v.recordSent(frame, bytes, now)
	c.pending = wire.PathChallengeFrame{}
	return nil
}

// response is the nominated-tuple receive helper. Other authenticated arrival
// tuples use responseProof directly. An unexpanded probe proves reachability
// but requires another probe to validate 1200 bytes.
func (c *migrationCandidate) response(tuple pathTuple, frame wire.PathResponseFrame, now time.Time) bool {
	canonical, err := canonicalPathTuple(tuple.local, tuple.remote)
	if err != nil || canonical != c.tuple || c.expire(now) {
		return false
	}
	return c.responseProof(frame, now)
}

// responseProof matches an authenticated token independently of its arrival
// tuple (RFC 9000 sections 8.2.2 and 8.2.3). Validation belongs to the probe.
func (c *migrationCandidate) responseProof(frame wire.PathResponseFrame, now time.Time) bool {
	if c.expire(now) {
		return false
	}
	matched, expanded := c.validation.matchResponse(frame, now)
	if !matched {
		return false
	}
	c.budget.Validate()
	if expanded {
		c.validation.state = PathValidated
		c.validation.probes = nil
		c.validation.pending = false
	} else {
		interval, deadline := c.validation.interval, c.validation.expires
		c.validation = pathValidation{state: PathValidating, interval: interval, expires: deadline}
	}
	c.pending = wire.PathChallengeFrame{}
	return expanded
}

// Bound local-candidate responses without replacing an unanswered token.
func (c *migrationCandidate) queueResponse(f wire.PathResponseFrame) error {
	if c.responsePending == nil {
		c.responsePending = &f
		return nil
	}
	if c.responseTail.count >= 7 {
		return ErrSendBuffer
	}
	c.responseTail.push(f)
	return nil
}
func (c *migrationCandidate) popResponse() {
	c.responsePending = nil
	if c.responseTail.count > 0 {
		f := c.responseTail.frames[0]
		c.responsePending = &f
		c.responseTail.pop()
	}
}
