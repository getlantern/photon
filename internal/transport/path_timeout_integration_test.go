package transport

import (
	"errors"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestMultipathConnectionTimeoutDispatch(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		client, _ := sendingPathPair(t, version)
		now := time.Unix(101, 0)
		client.config.Parameters.MaxIdleTimeout = 0
		client.peerParams.MaxIdleTimeout = 0
		client.timer.DiscardKeys()
		app := client.multipath.paths[1].application
		app.budget.Validate()
		if err := client.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
			t.Fatal(err)
		}
		if b, err := client.nextPacketOnPath(1, now); err != nil || b == nil {
			t.Fatal(err)
		}
		deadline := app.timer.Deadline()
		// Legacy pacing is no longer authoritative after multipath handoff.
		client.pacingAt = now.Add(-time.Second)
		for i := 0; i < 2; i++ {
			if got := client.RecoveryDeadline(); got != deadline {
				t.Fatal("combined deadline", got, deadline)
			}
		}
		if err := client.HandleTimeout(deadline.Add(-time.Nanosecond)); err != nil {
			t.Fatal(err)
		}
		if app.probePending {
			t.Fatal("early PTO")
		}
		if err := client.HandleTimeout(deadline); err != nil {
			t.Fatal(err)
		}
		if !app.probePending || client.probeAllowance[applicationSpace] {
			t.Fatal("used legacy timeout handler")
		}
		if got := client.RecoveryDeadline(); !got.IsZero() {
			t.Fatal("consumed deadline still armed", got)
		}
		if id, b, err := client.nextMultipathPacket(deadline); err != nil || id != 1 || b == nil {
			t.Fatal(id, err)
		}
		if !client.RecoveryDeadline().After(deadline) {
			t.Fatal("probe did not rearm")
		}
	}
}

func TestMultipathValidationPrecedesRecovery(t *testing.T) {
	client, _ := sendingPathPair(t, wire.Version1)
	now := time.Unix(101, 0)
	client.timer.DiscardKeys()
	app := client.multipath.paths[1].application
	app.budget.Validate()
	if err := client.validateMultipathPath(1); err != nil {
		t.Fatal(err)
	}
	if b, err := client.nextPacketOnPath(1, now); err != nil || b == nil {
		t.Fatal(err)
	}
	deadline := app.timer.Deadline()
	app.validation.expires = deadline
	app.validation.next = deadline
	if err := client.HandleTimeout(deadline); err != nil {
		t.Fatal(err)
	}
	if client.multipath.abandoned[1] == nil || app.probePending || !app.timer.Deadline().IsZero() {
		t.Fatal("failed path was probed")
	}
	if _, err := client.multipath.receiveAbandon(wire.PathAbandonFrame{PathID: 1}, deadline, time.Second); err != nil {
		t.Fatal(err)
	}
	expiry := deadline.Add(3 * time.Second)
	if got := client.RecoveryDeadline(); got != expiry {
		t.Fatal("retention missing", got, expiry)
	}
	if err := client.HandleTimeout(expiry); err != nil {
		t.Fatal(err)
	}
	if client.multipath.paths[1] != nil {
		t.Fatal("retained path not collected")
	}
}

func TestMultipathIdlePrecedesPathTimers(t *testing.T) {
	client, _ := sendingPathPair(t, wire.Version1)
	now := time.Unix(101, 0)
	client.config.Parameters.MaxIdleTimeout = 10000
	client.peerParams.MaxIdleTimeout = 10000
	client.idle = idleActivity{last: now}
	app := client.multipath.paths[1].application
	deadline := client.IdleDeadline()
	app.validation = &pathValidation{state: PathValidating, expires: deadline}
	paths := client.multipath
	if err := client.HandleTimeout(deadline); !errors.Is(err, ErrIdleTimeout) {
		t.Fatal(err)
	}
	if paths.abandoned[1] != nil || app.probePending {
		t.Fatal("idle expiry ran path timers")
	}
	if !client.RecoveryDeadline().IsZero() {
		t.Fatal("closed connection timer")
	}
}
