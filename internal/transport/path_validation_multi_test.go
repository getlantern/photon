package transport

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func commitPathProbe(t *testing.T, c *Conn, id uint64, size int, now time.Time) wire.PathChallengeFrame {
	t.Helper()
	app := c.multipath.paths[id].application
	for i, item := range app.space.queue {
		f, ok := item.frame.(wire.PathChallengeFrame)
		if !ok || item.acked {
			continue
		}
		app.validation.recordSent(f, size, now)
		c.queuedBytes -= item.bytes
		item.queued = false
		app.space.queue = append(app.space.queue[:i], app.space.queue[i+1:]...)
		return f
	}
	t.Fatal("no queued challenge")
	return wire.PathChallengeFrame{}
}

func TestEncryptedMultipathValidation(t *testing.T) {
	c, keys, cid := pathDecodeFixture(t, wire.Version1)
	now := time.Unix(101, 0)
	if err := c.validateMultipathPath(1); err != nil {
		t.Fatal(err)
	}
	app := c.multipath.paths[1].application
	count := len(app.space.queue)
	if err := c.validateMultipathPath(1); err != nil || len(app.space.queue) != count {
		t.Fatal("non-idempotent validation")
	}
	probe := app.space.queue[len(app.space.queue)-1].frame.(wire.PathChallengeFrame)
	payload, _ := (wire.PathResponseFrame{Data: probe.Data}).Append(nil)
	// A queued-but-unsent token is not evidence of reachability.
	if err := c.handleMultipathPacket(sealPathPayload(t, keys, cid, 1, 100, payload), now); err != nil {
		t.Fatal(err)
	}
	if app.budget.Validated() {
		t.Fatal("unsent challenge validated address")
	}
	commitPathProbe(t, c, 1, 1200, now)
	// A response with the right token on another path is not valid evidence.
	if err := c.handleMultipathPacket(sealPathPayload(t, keys, c.config.LocalCID, 0, 100, payload), now); err != nil {
		t.Fatal(err)
	}
	if app.budget.Validated() || app.validation.state != PathValidating {
		t.Fatal("response crossed paths")
	}
	if err := c.handleMultipathPacket(sealPathPayload(t, keys, cid, 1, 101, payload), now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if !app.budget.Validated() || app.validation.state != PathValidated {
		t.Fatal("matched expanded response did not validate")
	}
}

func TestMultipathValidationMinimumSizeAndExpiry(t *testing.T) {
	c, _, _ := pathDecodeFixture(t, wire.Version2)
	now := time.Unix(101, 0)
	if err := c.validateMultipathPath(1); err != nil {
		t.Fatal(err)
	}
	app := c.multipath.paths[1].application
	probe := commitPathProbe(t, c, 1, 100, now)
	if err := c.handlePathResponseOnPath(1, wire.PathResponseFrame{Data: probe.Data}, now.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if !app.budget.Validated() || app.validation.state != PathValidating || !app.validation.pending {
		t.Fatal("small probe validated MTU")
	}
	expanded := commitPathProbe(t, c, 1, 1200, now.Add(2*time.Millisecond))
	// At the exact expiry, even a correct response is too late.
	deadline := app.validation.expires
	if err := c.handlePathResponseOnPath(1, wire.PathResponseFrame{Data: expanded.Data}, deadline); err != nil {
		t.Fatal(err)
	}
	if app.validation.state != PathValidationFailed {
		t.Fatal("expired challenge accepted")
	}
}

func TestEncryptedChallengeResponseAffinity(t *testing.T) {
	c, keys, cid := pathDecodeFixture(t, wire.Version1)
	now := time.Unix(101, 0)
	before := len(c.spaces[applicationSpace].queue)
	challenge := wire.PathChallengeFrame{Data: [8]byte{1, 2, 3, 4}}
	payload, _ := challenge.Append(nil)
	if err := c.handleMultipathPacket(sealPathPayload(t, keys, cid, 1, 100, payload), now); err != nil {
		t.Fatal(err)
	}
	app := c.multipath.paths[1].application
	if len(app.space.queue) != 1 || len(c.spaces[applicationSpace].queue) != before {
		t.Fatal("response queued on wrong path")
	}
	response, ok := app.space.queue[0].frame.(wire.PathResponseFrame)
	if !ok || response.Data != challenge.Data {
		t.Fatal("response token mismatch")
	}
	if app.budget.Validated() {
		t.Fatal("receiving challenge validated peer")
	}
}

func TestMultipathValidationRetryBound(t *testing.T) {
	c, _, _ := pathDecodeFixture(t, wire.Version1)
	now := time.Unix(101, 0)
	if err := c.validateMultipathPath(1); err != nil {
		t.Fatal(err)
	}
	app := c.multipath.paths[1].application
	first := commitPathProbe(t, c, 1, 1200, now)
	for i := 0; i < 2; i++ {
		at := app.validation.next
		if err := c.expireValidationOnPath(1, at); err != nil {
			t.Fatal(err)
		}
		commitPathProbe(t, c, 1, 1200, at)
	}
	if len(app.validation.probes) != 3 {
		t.Fatal("unexpected probe count")
	}
	app.validation.recordSent(first, 1200, now)
	if len(app.validation.probes) != 3 {
		t.Fatal("unbounded sent-probe retention")
	}
	if err := c.expireValidationOnPath(1, app.validation.expires); err != nil {
		t.Fatal(err)
	}
	if app.validation.state != PathValidationFailed {
		t.Fatal("retry window did not end")
	}
}
