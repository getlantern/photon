package transport

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func TestKeepAliveDeadlinePolicy(t *testing.T) {
	now := time.Unix(100, 0)
	for _, tc := range []struct {
		local, peer  uint64
		period, want time.Duration
	}{
		{10000, 20000, time.Second, time.Second},
		{20000, 1000, time.Second, 500 * time.Millisecond},
		{1000, 0, time.Second, 500 * time.Millisecond},
		{0, 1000, time.Second, 500 * time.Millisecond},
		{0, 0, time.Second, time.Second},
		{1000, 1000, 0, 0},
	} {
		c := &Conn{recovery: &recovery.Sent{}, confirmed: true, config: Config{KeepAlivePeriod: tc.period, Parameters: wire.TransportParameters{MaxIdleTimeout: tc.local}}, peerParams: &wire.TransportParameters{MaxIdleTimeout: tc.peer}, idle: idleActivity{last: now}}
		got := c.keepAliveDeadline()
		if tc.want == 0 {
			if !got.IsZero() {
				t.Fatal("disabled", got)
			}
			continue
		}
		if got != now.Add(tc.want) {
			t.Fatal(tc, got)
		}
		c.idle.sent(now, true)
		if !c.keepAliveDeadline().IsZero() {
			t.Fatal("sent packet already requests response")
		}
		c.idle.received(now.Add(time.Second))
		if c.keepAliveDeadline() != now.Add(time.Second+tc.want) {
			t.Fatal("receive did not rearm")
		}
		c.confirmed = false
		if !c.keepAliveDeadline().IsZero() {
			t.Fatal("keepalive before confirmation")
		}
	}
}

func TestKeepAliveEncryptedExchangeAndSilentExpiry(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		c, peer := testPair(t, v, streamParameters())
		drivePair(t, c, peer, false, false)
		c.config.KeepAlivePeriod = time.Second
		c.config.Parameters.MaxIdleTimeout = 10000
		c.peerParams.MaxIdleTimeout = 10000
		now := c.idle.last.Add(time.Second)
		// Start from a successfully received packet, with no queued application work.
		c.idle.received(now)
		at := c.keepAliveDeadline()
		if c.RecoveryDeadline().After(at) {
			t.Fatal("runtime ignores keepalive")
		}
		if err := c.queueKeepAlive(at.Add(-time.Nanosecond)); err != nil {
			t.Fatal(err)
		}
		if c.queuedBytes != 0 {
			t.Fatal("early keepalive")
		}
		if err := c.HandleTimeout(at); err != nil {
			t.Fatal(err)
		}
		charged := c.queuedBytes
		if charged == 0 {
			t.Fatal("no PING queued")
		}
		if err := c.HandleTimeout(at); err != nil || c.queuedBytes != charged {
			t.Fatal("duplicate timer queued another PING", err)
		}
		packet, err := c.NextDatagram(at)
		if err != nil || len(packet) == 0 {
			t.Fatal("send", err)
		}
		if !c.keepAliveDeadline().IsZero() {
			t.Fatal("keepalive competes with recovery")
		}
		if err := peer.HandleDatagram(packet, at); err != nil {
			t.Fatal(err)
		}
		ackNow(peer)
		replies := drain(t, peer, at.Add(time.Millisecond))
		if len(replies) == 0 {
			t.Fatal("PING elicited no ACK")
		}
		for _, b := range replies {
			if err := c.HandleDatagram(b, at.Add(time.Millisecond)); err != nil {
				t.Fatal(err)
			}
		}
		if c.keepAliveDeadline().IsZero() || c.queuedBytes != 0 {
			t.Fatal("ACK did not rearm/refund")
		}
		next := c.keepAliveDeadline()
		if err := c.HandleTimeout(next); err != nil {
			t.Fatal(err)
		}
		if b, err := c.NextDatagram(next); err != nil || len(b) == 0 {
			t.Fatal("second PING", err)
		}
		// Lose all subsequent traffic: keepalive cannot keep a silent peer alive.
		deadline := c.IdleDeadline()
		if err := c.HandleTimeout(deadline); !errors.Is(err, ErrIdleTimeout) {
			t.Fatal("silent expiry", err)
		}
		if !c.keepAliveDeadline().IsZero() {
			t.Fatal("closed keepalive timer")
		}
	}
}

func TestKeepAliveQueueBackpressure(t *testing.T) {
	c, p := testPair(t, wire.Version1, streamParameters())
	drivePair(t, c, p, false, false)
	c.config.KeepAlivePeriod = time.Second
	c.idle.received(c.idle.last.Add(time.Second))
	at := c.keepAliveDeadline()
	before := c.IdleDeadline()
	c.queuedBytes = maxSendBytes
	if err := c.queueKeepAlive(at); err != nil {
		t.Fatal(err)
	}
	if c.IdleDeadline() != before || c.keepAliveDeadline() != at.Add(time.Second) {
		t.Fatal("blocked queue changed idle life or left due timer")
	}
	c.queuedBytes = 0
	if err := c.queueKeepAlive(at.Add(time.Second)); err != nil || c.queuedBytes == 0 {
		t.Fatal("retry", err)
	}
	queued := c.queuedBytes
	if err := c.queueKeepAlive(at.Add(2 * time.Second)); err != nil || c.queuedBytes != queued {
		t.Fatal("queued PING duplicated", err)
	}
}

func TestKeepAliveSelectsValidatedSurvivingPath(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		c, peer := sendingPathPair(t, v)
		if _, err := c.multipath.beginAbandon(0); err != nil {
			t.Fatal(err)
		}
		c.config.KeepAlivePeriod = time.Second
		c.idle.received(time.Unix(110, 0))
		at := c.keepAliveDeadline()
		if err := c.queueKeepAlive(at); err != nil {
			t.Fatal(err)
		}
		if c.queuedBytes != 0 {
			t.Fatal("queued on unvalidated survivor")
		}
		app := c.multipath.paths[1].application
		// Synthetic path fixture: test selection, not the validation exchange.
		app.validation.state = PathValidated
		app.budget.Validate()
		next := c.keepAliveDeadline()
		if err := c.queueKeepAlive(next); err != nil {
			t.Fatal(err)
		}
		if len(app.space.queue) != 1 {
			t.Fatal("no survivor keepalive")
		}
		if _, ok := app.space.queue[0].frame.(wire.PingFrame); !ok {
			t.Fatal("not PING")
		}
		id, b, err := c.nextMultipathPacket(next)
		if err != nil || id != 1 || len(b) == 0 {
			t.Fatal("survivor send", id, err)
		}
		decoded, err := peer.authenticateMultipathPacket(b, next)
		if err != nil || decoded == nil || !decoded.ackEliciting {
			t.Fatal("survivor authentication", err)
		}
		if !c.keepAliveDeadline().IsZero() {
			t.Fatal("sent survivor PING did not suppress keepalive")
		}
	}
}

func TestKeepAliveIgnoresBlockedPathQueue(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, blocked := range []uint64{0, 1} {
			c, peer := sendingPathPair(t, v)
			healthy := uint64(1) - blocked
			c.config.KeepAlivePeriod = time.Second
			c.idle.received(time.Unix(110, 0))
			if blocked == 0 {
				// An in-progress local migration holds ordinary output on path zero.
				c.multipath.paths[0].application.localMigration = &migrationCandidate{}
			}
			good := c.multipath.paths[healthy].application
			good.validation.state = PathValidated
			good.budget.Validate()
			if err := c.queueFrameOnPath(blocked, wire.PingFrame{}); err != nil {
				t.Fatal(err)
			}
			pending := c.multipath.paths[blocked].application.space.queue[0]
			before := c.queuedBytes
			at := c.keepAliveDeadline()
			if err := c.queueKeepAlive(at); err != nil {
				t.Fatal(err)
			}
			if len(good.space.queue) != 1 {
				t.Fatalf("version %x blocked path %d suppressed healthy keepalive", v, blocked)
			}
			if _, ok := good.space.queue[0].frame.(wire.PingFrame); !ok {
				t.Fatal("not PING")
			}
			if c.multipath.paths[blocked].application.space.queue[0] != pending || pending.acked || c.queuedBytes != before+32 {
				t.Fatal("changed blocked queue")
			}
			// Direct path output isolates keepalive selection from cross-path
			// queue borrowing. Normal congestion, pacing and encryption still apply.
			b, err := c.nextPacketOnPath(healthy, at)
			if err != nil || len(b) == 0 {
				t.Fatal("healthy output", err)
			}
			decoded, err := peer.authenticateMultipathPacket(b, at)
			if err != nil || decoded == nil || !decoded.ackEliciting {
				t.Fatal("authenticated PING", err)
			}
		}
	}
}

func TestKeepAlivePTOFloor(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(version), func(t *testing.T) {
			c, peer := testPair(t, version, streamParameters())
			drivePair(t, c, peer, false, false)
			c.config.KeepAlivePeriod = time.Millisecond
			c.config.Parameters.MaxIdleTimeout = 100
			c.peerParams.MaxIdleTimeout = 100
			c.peerParams.MaxAckDelay = 25
			now := time.Unix(101, 0)
			c.idle.received(now)
			// 200 ms SRTT + 4*50 ms variation + 25 ms ACK delay.
			c.recovery.RTT = recovery.RTT{Smoothed: 200 * time.Millisecond, Variation: 50 * time.Millisecond, Sampled: true}
			want := now.Add(637500 * time.Microsecond)
			if got := c.keepAliveDeadline(); got != want {
				t.Fatalf("PTO floor: got %v, want %v", got, want)
			}
			if !want.Before(c.IdleDeadline()) {
				t.Fatal("keepalive must precede the three-PTO idle floor")
			}
			if err := c.queueKeepAlive(want.Add(-time.Nanosecond)); err != nil || c.queuedBytes != 0 {
				t.Fatal("keepalive before PTO floor", err)
			}
			if err := c.queueKeepAlive(want); err != nil || c.queuedBytes == 0 {
				t.Fatal("missing due keepalive", err)
			}
			packet, err := c.NextDatagram(want)
			if err != nil || len(packet) == 0 {
				t.Fatal("keepalive not sent", err)
			}
			if err := peer.HandleDatagram(packet, want); err != nil {
				t.Fatal(err)
			}
			ackNow(peer)
			replies := drain(t, peer, want.Add(time.Millisecond))
			if len(replies) == 0 {
				t.Fatal("keepalive elicited no ACK")
			}
			for _, packet := range replies {
				if err := c.HandleDatagram(packet, want.Add(time.Millisecond)); err != nil {
					t.Fatal(err)
				}
			}
			// A new receive rearms the timer, and a changed RTT changes its floor.
			if c.idle.last != want.Add(time.Millisecond) || c.keepAliveDeadline().IsZero() {
				t.Fatal("authenticated ACK did not rearm keepalive")
			}
			c.recovery.RTT = recovery.RTT{Smoothed: time.Millisecond, Sampled: true}
			if got := c.keepAliveDeadline(); got != c.idle.last.Add(40500*time.Microsecond) {
				t.Fatal("stale PTO floor", got)
			}
			// Large path estimates must saturate, never wrap into an immediate
			// timer. PTO already saturates before the keepalive multiplier.
			const maxDuration = time.Duration(1<<63 - 1)
			c.recovery.RTT.Smoothed = maxDuration
			if got := c.keepAliveDeadline(); got != c.idle.last.Add(maxDuration) {
				t.Fatal("overflowed keepalive floor", got)
			}
		})
	}
}

func TestMultipathKeepAlivePTOSelection(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, fast := range []uint64{0, 1} {
			for _, blocked := range []string{"none", "migration", "mtu", "queued", "abandoned", "cid", "validation", "tie"} {
				if blocked == "validation" && fast == 0 {
					continue // Path zero was validated by the handshake.
				}
				t.Run(fmt.Sprintf("%s/fast=%d/%s", vName(version), fast, blocked), func(t *testing.T) {
					c, peer := sendingPathPair(t, version)
					c.config.KeepAlivePeriod = time.Millisecond
					c.config.Parameters.MaxIdleTimeout = 10000
					c.peerParams.MaxIdleTimeout = 10000
					c.peerParams.MaxAckDelay = 25
					now := time.Unix(110, 0)
					c.idle.received(now)
					for id, state := range c.multipath.paths {
						app := state.application
						app.validation.state = PathValidated
						app.budget.Validate()
						app.sent.RTT = recovery.RTT{Smoothed: time.Second, Variation: 100 * time.Millisecond, Sampled: true}
						if id == fast || blocked == "tie" {
							app.sent.RTT = recovery.RTT{Smoothed: 100 * time.Millisecond, Sampled: true}
						}
					}
					app := c.multipath.paths[fast].application
					selected, interval := fast, 189*time.Millisecond
					if blocked != "none" && blocked != "tie" {
						selected, interval = 1-fast, 2137500*time.Microsecond
					}
					switch blocked {
					case "migration":
						app.localMigration = &migrationCandidate{}
					case "mtu":
						app.mtuNeedsValidation = true
					case "queued":
						if err := c.queueFrameOnPath(fast, wire.PingFrame{}); err != nil {
							t.Fatal(err)
						}
					case "abandoned":
						if _, err := c.multipath.beginAbandon(fast); err != nil {
							t.Fatal(err)
						}
					case "cid":
						state := c.multipath.paths[fast]
						delete(state.peer.active, state.peer.current)
					case "validation":
						app.validation.state = PathUnchecked
					case "tie":
						selected = 0
					}
					at := now.Add(interval)
					if got := c.keepAliveDeadline(); got != at {
						t.Fatalf("deadline %v, want %v", got, at)
					}
					before := c.queuedBytes
					if err := c.queueKeepAlive(at.Add(-time.Nanosecond)); err != nil || c.queuedBytes != before {
						t.Fatal("early probe", err)
					}
					if err := c.queueKeepAlive(at); err != nil {
						t.Fatal(err)
					}
					if len(c.multipath.paths[selected].application.space.queue) != 1 {
						t.Fatal("probe queued on wrong path")
					}
					// Without unrelated queued work, exercise the real scheduler too:
					// a lower-ID slow path must not borrow this path-affine PING.
					var packet []byte
					var err error
					if blocked == "none" || blocked == "tie" {
						var actual uint64
						actual, packet, err = c.nextMultipathPacket(at)
						if actual != selected {
							t.Fatalf("scheduled path %d, want %d", actual, selected)
						}
					} else {
						packet, err = c.nextPacketOnPath(selected, at)
					}
					if err != nil || len(packet) == 0 {
						t.Fatal("probe output", err)
					}
					decoded, err := peer.authenticateMultipathPacket(packet, at)
					if err != nil || decoded == nil || !decoded.ackEliciting {
						t.Fatal("probe authentication", err)
					}
					if !c.keepAliveDeadline().IsZero() {
						t.Fatal("probe did not suppress further keepalive")
					}
				})
			}
		}
	}
}
