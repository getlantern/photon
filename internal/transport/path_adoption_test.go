package transport

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func TestPathZeroHandoff(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := testPair(t, v)
		drivePair(t, client, server, false, false)
		for _, c := range []*Conn{client, server} {
			now := time.Unix(101, 0)
			if err := c.enqueue(applicationSpace, wire.PingFrame{}, 1); err != nil {
				t.Fatal(err)
			}
			if len(drain(t, c, now)) == 0 {
				t.Fatal("no outstanding application packet")
			}
			if err := c.enqueue(applicationSpace, wire.PingFrame{}, 1); err != nil {
				t.Fatal(err)
			}
			space := &c.spaces[applicationSpace]
			next, expected, flight := space.nextPN, space.received.Expected(), c.recovery.BytesInFlight
			deadline, backoff, window, rtt := c.timer.Deadline(), c.timer.Backoff(), c.congestion.Window(), c.recovery.RTT
			sentCount, queueCount := len(space.sent), len(space.queue)
			if next == 0 || expected == 0 || flight == 0 || sentCount == 0 || queueCount == 0 {
				t.Fatal("handoff fixture has no state")
			}
			p, err := adoptPathZero(c, 2, 2, 3)
			if err != nil {
				t.Fatal(err)
			}
			app, err := p.application(0)
			if err != nil {
				t.Fatal(err)
			}
			if app.space != space || app.sent != c.recovery || app.congestion != c.congestion || app.timer != &c.timer || app.pacer != &c.pacer {
				t.Fatal("handoff cloned state")
			}
			if app.space.nextPN != next || app.space.received.Expected() != expected || app.sent.BytesInFlight != flight || app.timer.Deadline() != deadline || app.timer.Backoff() != backoff || app.congestion.Window() != window || app.sent.RTT != rtt {
				t.Fatal("handoff reset transport state")
			}
			if len(app.space.sent) != sentCount || len(app.space.queue) != queueCount {
				t.Fatal("handoff lost reliable frames")
			}
			if p.paths[0].local != c.localIDs || p.paths[0].peer != c.peerIDs {
				t.Fatal("handoff cloned CID ownership")
			}
			for seq, id := range c.peerIDs.active {
				binding := pathCIDBinding{sequence: seq}
				if p.peerByCID[string(id.cid)] != binding || p.peerHistory[binding].hasToken != id.hasToken || p.peerHistory[binding].token != id.token {
					t.Fatal("lost peer CID binding/token")
				}
			}
			// Further path-zero sends keep the existing ledger and number sequence.
			if err := app.recordSent(recovery.Packet{Number: next, SentAt: now, Bytes: 1200, AckEliciting: true, InFlight: true}); err != nil {
				t.Fatal(err)
			}
			if space.nextPN != next+1 || space.largestSent != next || !space.sentAny {
				t.Fatal("send number not shared")
			}
			events, err := p.receiveACK(wire.PathAckFrame{AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: next, Largest: next}}}}, now.Add(time.Millisecond), 3, 25*time.Millisecond, false)
			if err != nil || len(events.Acked) != 1 || events.Acked[0].Number != next {
				t.Fatal(events, err)
			}
			// Additional paths retain fresh spaces; creating one cannot reset path zero.
			if _, err := p.issue(1); err != nil {
				t.Fatal(err)
			}
			other, err := p.application(1)
			if err != nil || other.space.nextPN != 0 || other.sent == app.sent || other.space == space || other.timer == app.timer {
				t.Fatal("new path inherited path-zero state", err)
			}
		}
	}
}

func TestPathZeroHandoffGuards(t *testing.T) {
	if _, err := adoptPathZero(nil, 1, 1, 2); err == nil {
		t.Fatal("nil connection")
	}
	client, server := testPair(t, wire.Version1)
	if _, err := adoptPathZero(client, 1, 1, 2); err == nil {
		t.Fatal("unconfirmed handoff")
	}
	drivePair(t, client, server, false, false)
	if _, err := adoptPathZero(client, 1<<32, 1, 2); err == nil {
		t.Fatal("invalid path maximum")
	}
	if err := client.RotatePeerID(); err != nil {
		t.Fatal(err)
	}
	if _, err := adoptPathZero(client, 1, 1, 2); err == nil {
		t.Fatal("handoff discarded retired CID history")
	}
	server.Close()
	if _, err := adoptPathZero(server, 1, 1, 2); err == nil {
		t.Fatal("closed connection handoff")
	}
}

func TestPathZeroHandoffPendingProbe(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	now := time.Unix(101, 0)
	if err := client.enqueue(applicationSpace, wire.PingFrame{}, 1); err != nil {
		t.Fatal(err)
	}
	if len(drain(t, client, now)) == 0 {
		t.Fatal("no outstanding packet")
	}
	deadline := client.timer.Deadline()
	if err := client.HandleTimeout(deadline); err != nil {
		t.Fatal(err)
	}
	if !client.probeAllowance[applicationSpace] {
		t.Fatal("missing pending probe")
	}
	client.recovery.BytesInFlight = client.congestion.Window()
	client.pacingAt = deadline.Add(time.Second)
	p, err := adoptPathZero(client, 1, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	client.multipath = p
	app := p.paths[0].application
	if !app.probePending || app.pacingAt != client.pacingAt {
		t.Fatal("handoff lost send readiness")
	}
	before := app.space.nextPN
	id, b, err := client.nextMultipathPacket(deadline)
	if err != nil || id != 0 || b == nil {
		t.Fatal("handoff stranded PTO at congestion window", id, err)
	}
	if app.probePending || app.space.nextPN != before+1 || !app.timer.Deadline().After(deadline) {
		t.Fatal("probe commit after handoff")
	}
	if _, err := adoptPathZero(client, 1, 1, 2); err == nil {
		t.Fatal("duplicate handoff replaced ownership")
	}
}
