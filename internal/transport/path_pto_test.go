package transport

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func attachSlowPath(t *testing.T, c *Conn) *pathApplication {
	t.Helper()
	p, err := adoptPathZero(c, 3, 3, 4)
	if err != nil {
		t.Fatal(err)
	}
	c.multipath = p
	if _, err = p.issue(1); err != nil {
		t.Fatal(err)
	}
	a, err := p.application(1)
	if err != nil {
		t.Fatal(err)
	}
	a.sent.RTT = recovery.RTT{Smoothed: time.Second, Variation: 100 * time.Millisecond, Sampled: true}
	return a
}

func TestMultipathRetentionPTO(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	base := client.retentionPTO(true, 25*time.Millisecond)
	slow := attachSlowPath(t, client)
	want := 1425 * time.Millisecond
	if base >= want || client.retentionPTO(true, 25*time.Millisecond) != want {
		t.Fatal("largest path not selected")
	}
	// CID-only paths must not add a default RTT before network activation.
	if _, err := client.multipath.issue(2); err != nil {
		t.Fatal(err)
	}
	if client.retentionPTO(true, 25*time.Millisecond) != want {
		t.Fatal("CID-only path changed PTO")
	}
	now := time.Unix(101, 0)
	client.armPreviousKeys(now)
	if !client.applicationKeys.previousExpiry.Equal(now.Add(3 * want)) {
		t.Fatal("key retention uses primary path")
	}
	client.multipath.beginAbandon(1)
	client.multipath.receiveAbandon(wire.PathAbandonFrame{PathID: 1}, now, time.Second)
	if client.retentionPTO(true, 25*time.Millisecond) != want {
		t.Fatal("retained abandoned path omitted")
	}
	client.multipath.collectAbandoned(now.Add(3 * time.Second))
	if client.retentionPTO(true, 25*time.Millisecond) != base {
		t.Fatal("disposed path still affects retention")
	}
	// Duration arithmetic saturates instead of wrapping to an expired deadline.
	client.multipath.paths[2].application = slow
	slow.sent.RTT.Smoothed = time.Duration(1<<63 - 1)
	client.armPreviousKeys(now)
	if !client.applicationKeys.previousExpiry.Equal(now.Add(time.Duration(1<<63 - 1))) {
		t.Fatal("retention overflow")
	}
}

func TestMultipathClosingRetention(t *testing.T) {
	for _, draining := range []bool{false, true} {
		client, server := testPair(t, wire.Version2)
		drivePair(t, client, server, false, false)
		attachSlowPath(t, client)
		now := time.Unix(101, 0)
		want := now.Add(3 * 1425 * time.Millisecond)
		if draining {
			client.beginDraining(ErrPeerClosed, now)
			if !client.drainUntil.Equal(want) {
				t.Fatal("draining ended too early")
			}
		} else {
			// Use the existing public engine close path to exercise retention selection.
			if err := client.CloseWithError(0, "", now); err != nil {
				t.Fatal(err)
			}
			if !client.closing.until.Equal(want) {
				t.Fatal("closing ended too early")
			}
		}
	}
}

func TestMultipathIdlePTOFloor(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := testPair(t, version)
		drivePair(t, client, server, false, false)
		client.config.Parameters.MaxIdleTimeout = 1
		client.peerParams.MaxIdleTimeout = 1
		client.peerParams.MaxAckDelay = 25
		now := time.Unix(101, 0)
		client.idle.received(now)
		base := client.IdleDeadline()
		slow := attachSlowPath(t, client)
		want := now.Add(3 * 1425 * time.Millisecond)
		if !base.Before(want) || client.IdleDeadline() != want {
			t.Fatal("idle floor ignored slow path", base, client.IdleDeadline(), want)
		}
		// PTO backoff must not keep an idle connection alive indefinitely.
		if err := slow.sent.OnSent(recovery.Application, recovery.Packet{Number: 0, SentAt: now, Bytes: 1200, AckEliciting: true, InFlight: true}); err != nil {
			t.Fatal(err)
		}
		slow.timer.Arm(slow.sent, now, recovery.TimerState{Confirmed: true, PeerValidated: true, MaxACKDelay: 25 * time.Millisecond})
		if timeout, ok := slow.timer.Expire(slow.sent, slow.timer.Deadline()); !ok || !timeout.Probe || slow.timer.Backoff() == 0 {
			t.Fatal("PTO did not back off")
		}
		if client.IdleDeadline() != want {
			t.Fatal("probe backoff extended idle timeout")
		}
		// A larger negotiated idle timeout continues to take precedence.
		client.config.Parameters.MaxIdleTimeout = 10000
		client.peerParams.MaxIdleTimeout = 10000
		if client.IdleDeadline() != now.Add(10*time.Second) {
			t.Fatal("negotiated idle timeout lost")
		}
		client.config.Parameters.MaxIdleTimeout = 0
		client.peerParams.MaxIdleTimeout = 0
		if !client.IdleDeadline().IsZero() {
			t.Fatal("disabled idle timeout enabled")
		}
	}
}
