package transport

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func pathRecoveryPair(t *testing.T) (*pathCIDs, *pathApplication, *pathApplication) {
	t.Helper()
	p := testPathCIDs(t, 3)
	if _, err := p.issue(1); err != nil {
		t.Fatal(err)
	}
	if p.paths[0].application != nil || p.paths[1].application != nil {
		t.Fatal("eager packet state allocation")
	}
	a, err := p.application(0)
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.application(1)
	if err != nil {
		t.Fatal(err)
	}
	return p, a, b
}
func sendPathPacket(t *testing.T, a *pathApplication, now time.Time) {
	t.Helper()
	if err := a.recordSent(recovery.Packet{Number: a.space.nextPN, SentAt: now, Bytes: 1200, AckEliciting: true, InFlight: true}); err != nil {
		t.Fatal(err)
	}
}
func TestPathPacketNumberIsolation(t *testing.T) {
	p, a, b := pathRecoveryPair(t)
	// NewReno cuts the window of the path whose packets were lost.
	useNewRenoPaths(t, p)
	now := time.Unix(100, 0)
	for _, app := range []*pathApplication{a, b} {
		for i := 0; i < 8; i++ {
			sendPathPacket(t, app, now)
		}
		receipt, err := app.space.received.Record(0, now)
		if err != nil || receipt != NewPacket {
			t.Fatal(receipt, err)
		}
	}
	for id, app := range []*pathApplication{a, b} {
		f, err := app.ack(uint64(id), now, 3)
		if err != nil || f.PathID != uint64(id) || !f.Acknowledges(0) {
			t.Fatal(f, err)
		}
	}
	f := wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 7, Largest: 7}}}}
	events, err := p.receiveACK(f, now.Add(10*time.Millisecond), 3, 25*time.Millisecond, false)
	if err != nil || len(events.Acked) != 1 || len(events.Lost) != 5 {
		t.Fatal(events, err)
	}
	if b.sent.BytesInFlight != 2400 || a.sent.BytesInFlight != 9600 {
		t.Fatal("ACK/loss crossed paths")
	}
	if a.sent.RTT.Sampled || !b.sent.RTT.Sampled || a.congestion.Window() != recovery.InitialWindow(1200) || b.congestion.Window() >= recovery.InitialWindow(1200) {
		t.Fatal("RTT/congestion crossed paths")
	}
	// A base ACK names path zero, even when received on another network path.
	events, err = p.receiveACK(wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 7}}}, now.Add(20*time.Millisecond), 3, 25*time.Millisecond, false)
	if err != nil || len(events.Acked) != 8 || a.sent.BytesInFlight != 0 || b.sent.BytesInFlight != 2400 {
		t.Fatal(events, err)
	}
	if _, err := a.space.received.Record(1, now); err != nil {
		t.Fatal(err)
	}
	if b.space.received.Expected() != 1 || a.space.received.Expected() != 2 {
		t.Fatal("receive spaces shared")
	}
	f.Ranges = []wire.AckRange{{Smallest: 8, Largest: 8}}
	_, err = p.receiveACK(f, now, 3, time.Millisecond, false)
	requirePathError(t, err, 0xa, f.Type())
	if b.sent.BytesInFlight != 2400 {
		t.Fatal("invalid ACK changed flight")
	}
	old := a.space.nextPN
	if err := a.recordSent(recovery.Packet{Number: old + 1}); err == nil || a.space.nextPN != old {
		t.Fatal("failed send consumed packet number")
	}
}

func TestPathPTOIsolation(t *testing.T) {
	_, a, b := pathRecoveryPair(t)
	now := time.Unix(100, 0)
	sendPathPacket(t, a, now)
	sendPathPacket(t, b, now.Add(time.Second))
	a.arm(now, 25*time.Millisecond, false)
	b.arm(now, 25*time.Millisecond, false)
	if !a.timer.Deadline().Before(b.timer.Deadline()) {
		t.Fatal("path deadlines shared")
	}
	event, due := a.timer.Expire(a.sent, a.timer.Deadline())
	if !due || !event.Probe || len(event.Lost) != 0 || a.timer.Backoff() != 1 || b.timer.Backoff() != 0 {
		t.Fatal("PTO crossed paths")
	}
	if a.sent.BytesInFlight != 1200 || b.sent.BytesInFlight != 1200 {
		t.Fatal("PTO declared loss")
	}
}

func TestAbandonedPathLateACKAndRecovery(t *testing.T) {
	p, a, b := pathRecoveryPair(t)
	now := time.Unix(100, 0)
	sendPathPacket(t, a, now)
	sendPathPacket(t, b, now)
	sendPathPacket(t, b, now)
	b.arm(now, 0, false)
	p.beginAbandon(1)
	p.receiveAbandon(wire.PathAbandonFrame{PathID: 1}, now, time.Second)
	if _, err := p.application(1); err == nil {
		t.Fatal("abandoned path reactivated")
	}
	f := wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}}}
	events, err := p.receiveACK(f, now.Add(time.Millisecond), 3, 0, false)
	if err != nil || len(events.Acked) != 1 || b.sent.BytesInFlight != 1200 {
		t.Fatal("late ACK lost", events, err)
	}
	disposed := p.collectAbandoned(now.Add(3 * time.Second))
	if len(disposed) != 1 || disposed[0].ID != 1 || len(disposed[0].Lost) != 1 || disposed[0].Lost[0].Number != 1 {
		t.Fatal("remaining transmission not declared lost", disposed)
	}
	if b.sent.BytesInFlight != 0 || !b.timer.Deadline().IsZero() || a.sent.BytesInFlight != 1200 {
		t.Fatal("disposal accounting")
	}
	if events, err = p.receiveACK(f, now.Add(4*time.Second), 3, 0, false); err != nil || len(events.Acked) != 0 {
		t.Fatal("ACK after disposal", err)
	}
	f.PathID = 2
	_, err = p.receiveACK(f, now, 3, 0, false)
	requirePathError(t, err, 0xa, f.Type())
}
