package transport

import (
	"errors"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestFixedPathValidation(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		c, s := testPair(t, v)
		drivePair(t, c, s, false, false)
		if err := c.ValidatePath(); err != nil {
			t.Fatal(err)
		}
		if err := c.ValidatePath(); err != nil {
			t.Fatal(err)
		}
		if len(c.spaces[applicationSpace].queue) != 1 {
			t.Fatal("duplicate initiation")
		}
		drivePair(t, c, s, false, false)
		if c.PathValidationState() != PathValidated {
			t.Fatal("path not validated")
		}
		if !c.pathValidationDeadline().IsZero() {
			t.Fatal("validation timer retained")
		}
	}
}

func TestPathValidationRequiresSentToken(t *testing.T) {
	c, s := testPair(t, wire.Version1)
	drivePair(t, c, s, false, false)
	c.ValidatePath()
	f := c.spaces[applicationSpace].queue[0].frame.(wire.PathChallengeFrame)
	c.budget = Amplification{}
	c.handlePathResponse(wire.PathResponseFrame{Data: f.Data}, time.Unix(200, 0))
	if c.budget.Validated() || c.PathValidationState() != PathValidating {
		t.Fatal("unsent token validated")
	}
	if !c.pathValidationDeadline().IsZero() {
		t.Fatal("timer armed before actual transmission")
	}
}

func TestPathValidationRetriesAndExpiry(t *testing.T) {
	c, s := testPair(t, wire.Version1)
	drivePair(t, c, s, false, false)
	now := time.Unix(200, 0)
	c.ValidatePath()
	seen := map[[8]byte]bool{}
	for i := 0; i < 3; i++ {
		if i > 0 {
			now = c.validation.next
			if err := c.HandleTimeout(now); err != nil {
				t.Fatal(err)
			}
		}
		drain(t, c, now) // Deliberately drop every transmission.
		if len(c.validation.probes) != i+1 {
			t.Fatal("missing challenge", i)
		}
		token := c.validation.probes[i].token
		if seen[token] {
			t.Fatal("challenge token reused")
		}
		seen[token] = true
	}
	expires := c.validation.expires
	c.HandleTimeout(expires)
	if c.PathValidationState() != PathValidationFailed || c.err != nil {
		t.Fatal("failed check closed live connection")
	}
	for token := range seen {
		c.handlePathResponse(wire.PathResponseFrame{Data: token}, expires)
	}
	if c.PathValidationState() != PathValidationFailed {
		t.Fatal("expired token accepted")
	}
	for _, item := range c.spaces[applicationSpace].queue {
		if _, ok := item.frame.(wire.PathChallengeFrame); ok {
			t.Fatal("expired challenge retained")
		}
	}
}

func TestUnexpandedProbeRequiresMTUValidation(t *testing.T) {
	c, s := testPair(t, wire.Version1)
	drivePair(t, c, s, false, false)
	now := time.Unix(200, 0)
	c.budget = Amplification{credit: 100}
	c.ValidatePath()
	b, err := c.NextDatagram(now)
	if err != nil || len(b) == 0 || len(b) >= 1200 {
		t.Fatal("unexpanded challenge", len(b), err)
	}
	token := c.validation.probes[0].token
	if err := c.handlePathResponse(wire.PathResponseFrame{Data: token}, now); err != nil {
		t.Fatal(err)
	}
	if !c.budget.Validated() || c.PathValidationState() != PathValidating {
		t.Fatal("address/MTU distinction")
	}
	b, err = c.NextDatagram(now.Add(time.Second))
	if err != nil || len(b) != 1200 {
		t.Fatal("missing expanded second check", len(b), err)
	}
	token2 := c.validation.probes[0].token
	if token == token2 {
		t.Fatal("reused small probe token")
	}
	c.handlePathResponse(wire.PathResponseFrame{Data: token}, now.Add(time.Second))
	if c.PathValidationState() != PathValidating {
		t.Fatal("small probe validated MTU")
	}
	c.handlePathResponse(wire.PathResponseFrame{Data: token2}, now.Add(time.Second))
	if c.PathValidationState() != PathValidated {
		t.Fatal("expanded probe did not validate")
	}
}

func TestChallengeACKDoesNotValidatePath(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		c, s := testPair(t, v)
		drivePair(t, c, s, false, false)
		now := time.Unix(200, 0)
		// Enough anti-amplification credit for a padded challenge, still unvalidated.
		c.budget = Amplification{credit: 1200}
		if err := c.ValidatePath(); err != nil {
			t.Fatal(err)
		}
		pn := c.spaces[applicationSpace].nextPN
		if b, err := c.NextDatagram(now); err != nil || len(b) != 1200 {
			t.Fatal("challenge send", len(b), err)
		}
		deadline := c.pathValidationDeadline()
		token := c.validation.probes[0].token
		ack := makePeerPacket(t, s, applicationSpace, 1000, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: pn, Largest: pn}}})
		if err := c.HandleDatagram(ack, now.Add(time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		if c.budget.Validated() || c.PathValidationState() != PathValidating || c.pathValidationDeadline() != deadline {
			t.Fatal("ACK validated or postponed path check")
		}
		wrong := token
		wrong[0] ^= 1
		response := makePeerPacket(t, s, applicationSpace, 1001, wire.PathResponseFrame{Data: wrong})
		if err := c.HandleDatagram(response, now.Add(2*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		if c.budget.Validated() || c.PathValidationState() != PathValidating {
			t.Fatal("wrong token validated")
		}
		response = makePeerPacket(t, s, applicationSpace, 1002, wire.PathResponseFrame{Data: token})
		if err := c.HandleDatagram(response, now.Add(3*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		if !c.budget.Validated() || c.PathValidationState() != PathValidated {
			t.Fatal("matching response rejected")
		}
	}
}

func TestValidatePathSendBufferIsRecoverable(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		c, s := testPair(t, v)
		drivePair(t, c, s, false, false)
		// Queued application traffic leaves less than one challenge of room.
		held := c.availableSendBytes() - 31
		c.queuedBytes += held
		if err := c.ValidatePath(); !errors.Is(err, ErrSendBuffer) {
			t.Fatal("full buffer admitted validation", err)
		}
		if c.err != nil || c.Closing() || c.check() != nil {
			t.Fatal("optional validation failed the connection", c.err)
		}
		if c.PathValidationState() != PathUnchecked || len(c.spaces[applicationSpace].queue) != 0 || !c.pathValidationDeadline().IsZero() {
			t.Fatal("rejected validation changed state")
		}
		c.queuedBytes -= held
		if err := c.ValidatePath(); err != nil {
			t.Fatal(err)
		}
		drivePair(t, c, s, false, false)
		if c.PathValidationState() != PathValidated {
			t.Fatal("retry after freeing capacity did not validate")
		}
	}
}
