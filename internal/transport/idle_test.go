package transport

import (
	"errors"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func TestIdleActivitySendRule(t *testing.T) {
	now := time.Unix(100, 0)
	var activity idleActivity
	activity.received(now)
	activity.sent(now.Add(time.Second), false)
	if activity.last != now {
		t.Fatal("ACK-only send extended idle timer")
	}
	activity.sent(now.Add(2*time.Second), true)
	activity.sent(now.Add(3*time.Second), true)
	if activity.last != now.Add(2*time.Second) {
		t.Fatal("repeated send extended idle timer")
	}
	activity.received(now.Add(4 * time.Second))
	activity.sent(now.Add(5*time.Second), true)
	if activity.last != now.Add(5*time.Second) {
		t.Fatal("first send after receive did not extend timer")
	}
	activity.received(now)
	if activity.last != now.Add(5*time.Second) {
		t.Fatal("timestamp regressed")
	}
}
func TestIdleTimeoutNegotiation(t *testing.T) {
	now := time.Unix(100, 0)
	for _, tc := range []struct {
		local, peer uint64
		want        time.Duration
	}{
		{0, 0, 0}, {10000, 0, 10 * time.Second}, {0, 20000, 20 * time.Second}, {10000, 20000, 10 * time.Second}, {20000, 10000, 10 * time.Second}, {1, 2, 2997 * time.Millisecond},
	} {
		r, _ := recovery.NewSent(4)
		p := wire.DefaultTransportParameters()
		p.MaxIdleTimeout = tc.peer
		c := &Conn{config: Config{Parameters: wire.TransportParameters{MaxIdleTimeout: tc.local}}, peerParams: &p, recovery: r, idle: idleActivity{last: now}}
		got := c.IdleDeadline()
		if tc.want == 0 {
			if !got.IsZero() {
				t.Fatal(tc, got)
			}
		} else if got != now.Add(tc.want) {
			t.Fatal(tc, got)
		}
	}
	if milliseconds(wire.MaxVarint) != time.Duration(1<<63-1) || triple(time.Duration(1<<63-1)) != time.Duration(1<<63-1) {
		t.Fatal("timeout duration overflow")
	}
}
func TestIdleTimeoutIsSilentAndSticky(t *testing.T) {
	p := streamParameters()
	p.MaxIdleTimeout = 1
	client, _ := testPair(t, wire.Version1, p)
	now := time.Unix(100, 0)
	drain(t, client, now)
	deadline := client.IdleDeadline()
	if deadline != now.Add(2997*time.Millisecond) {
		t.Fatal("initial three-PTO floor", deadline)
	}
	// PTO traffic may be sent, but cannot keep a silent peer alive indefinitely.
	if err := client.HandleTimeout(now.Add(999 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	drain(t, client, now.Add(999*time.Millisecond))
	if client.IdleDeadline() != deadline {
		t.Fatal("probe moved idle deadline")
	}
	if err := client.HandleTimeout(deadline); !errors.Is(err, ErrIdleTimeout) {
		t.Fatal(err)
	}
	if b, err := client.NextDatagram(deadline); len(b) != 0 || !errors.Is(err, ErrIdleTimeout) {
		t.Fatal("idle timeout sent output", err)
	}
	if !client.RecoveryDeadline().IsZero() {
		t.Fatal("idle timeout retained timer")
	}
}
func TestAuthenticatedReceiveExtendsIdle(t *testing.T) {
	p := streamParameters()
	p.MaxIdleTimeout = 10000
	client, server := testPair(t, wire.Version1, p)
	drivePair(t, client, server, false, false)
	before := client.IdleDeadline()
	now := before.Add(-time.Second)
	if err := client.HandleDatagram([]byte("unrelated garbage"), now); err != nil {
		t.Fatal(err)
	}
	if client.IdleDeadline() != before {
		t.Fatal("unauthenticated input extended timer")
	}
	b := makePeerPacket(t, server, applicationSpace, 1000, wire.PingFrame{})
	if err := client.HandleDatagram(b, now); err != nil {
		t.Fatal(err)
	}
	if client.IdleDeadline() != now.Add(10*time.Second) {
		t.Fatal("authenticated input did not extend timer")
	}
}
