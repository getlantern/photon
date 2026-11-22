package transport

import (
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestMigrationCandidateValidation(t *testing.T) {
	tuple := pathTuple{netip.MustParseAddrPort("192.0.2.1:443"), netip.MustParseAddrPort("192.0.2.2:444")}
	now := time.Unix(101, 0)
	c, err := newMigrationCandidate(tuple, 20, now, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	frame, ready, err := c.challenge(now)
	if err != nil || !ready {
		t.Fatal(err)
	}
	response := wire.PathResponseFrame{Data: frame.Data}
	if c.response(tuple, response, now) || c.budget.Validated() {
		t.Fatal("unsent token validated")
	}
	if err := c.sent(frame, 1200, now); !errors.Is(err, ErrAmplification) {
		t.Fatal(err)
	}
	repeated, ready, err := c.challenge(now)
	if err != nil || !ready || repeated != frame || len(c.validation.probes) != 0 {
		t.Fatal("blocked probe changed token/history")
	}
	if err := c.sent(frame, 60, now); err != nil {
		t.Fatal(err)
	}
	wrong := tuple
	wrong.remote = netip.MustParseAddrPort("192.0.2.2:445")
	if c.response(wrong, response, now) || c.budget.Validated() {
		t.Fatal("wrong tuple validated")
	}
	if c.response(tuple, response, now) || !c.budget.Validated() {
		t.Fatal("short probe must prove address but not MTU")
	}
	deadline := c.validation.expires
	frame, ready, err = c.challenge(now.Add(time.Millisecond))
	if err != nil || !ready || frame.Data == response.Data {
		t.Fatal("missing fresh MTU probe")
	}
	if err := c.sent(frame, 60, now); err == nil {
		t.Fatal("accepted unpadded probe with sufficient credit")
	}
	if err := c.sent(frame, 1200, now); err != nil {
		t.Fatal(err)
	}
	if !c.response(tuple, wire.PathResponseFrame{Data: frame.Data}, now.Add(time.Millisecond)) || c.validation.state != PathValidated {
		t.Fatal("expanded probe did not validate")
	}
	if c.validation.expires != deadline {
		t.Fatal("response extended lifetime")
	}
}

func TestMigrationCandidateBoundedLifetime(t *testing.T) {
	tuple := pathTuple{netip.MustParseAddrPort("192.0.2.1:443"), netip.MustParseAddrPort("192.0.2.2:444")}
	now := time.Unix(101, 0)
	c, err := newMigrationCandidate(tuple, 1200, now, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var last wire.PathChallengeFrame
	for i := 0; i < 3; i++ {
		at := now.Add(time.Duration(i) * time.Second)
		frame, ready, err := c.challenge(at)
		if err != nil || !ready {
			t.Fatal(i, err)
		}
		if err := c.sent(frame, 1200, at); err != nil {
			t.Fatal(err)
		}
		last = frame
		if _, ready, err := c.challenge(at); err != nil || ready {
			t.Fatal("immediate retry allowed")
		}
	}
	deadline := now.Add(3 * time.Second)
	if err := c.credit(tuple, 1200, deadline.Add(-time.Nanosecond)); err != nil {
		t.Fatal(err)
	}
	if c.response(tuple, wire.PathResponseFrame{Data: last.Data}, deadline) {
		t.Fatal("expired response validated")
	}
	if c.validation.state != PathValidationFailed || len(c.validation.probes) != 0 {
		t.Fatal("expired candidate retained tokens")
	}
	if _, ready, err := c.challenge(deadline); err != nil || ready {
		t.Fatal("expired candidate retried")
	}
	// Expiry also bounds a candidate that never gets enough credit to send.
	blocked, _ := newMigrationCandidate(tuple, 1, now, time.Second)
	if !blocked.expire(deadline) {
		t.Fatal("unsent candidate retained forever")
	}
}
