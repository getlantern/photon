package transport

import (
	"bytes"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

// openController never limits sending, so only the packet ledger can.
type openController struct{ recovery.Controller }

func (openController) CanSend(uint64, int, bool, bool) bool { return true }
func (openController) PacingRate() uint64                   { return 1 << 40 }

// ledgerPair is an established pair whose client has stream data queued and
// a controller that never limits it.
func ledgerPair(t *testing.T) *Conn {
	t.Helper()
	client, server := testPair(t, wire.Version1, streamParameters())
	drivePair(t, client, server, false, false)
	client.congestion = openController{client.congestion}
	id, err := client.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.WriteStream(id, bytes.Repeat([]byte{1}, 4000), false); err != nil {
		t.Fatal(err)
	}
	return client
}

// A packet ledger full of packets in flight makes the sender wait for ACKs:
// no packet carrying frames goes, and the connection stays open. It used to
// fail with ErrSendBuffer.
func TestFullSentLedgerWaits(t *testing.T) {
	c := ledgerPair(t)
	s := &c.spaces[applicationSpace]
	now := time.Unix(200, 0)
	for len(s.sent) < maxSentPackets {
		pn := s.nextPN
		if err := c.recovery.OnSent(recovery.Application, recovery.Packet{Number: pn, SentAt: now, Bytes: 1200, AckEliciting: true, InFlight: true}); err != nil {
			t.Fatal(err)
		}
		s.sent[pn] = &sendItem{frame: wire.PingFrame{}, transmitted: true}
		s.nextPN++
	}
	pn := s.nextPN
	out, err := c.NextDatagram(now)
	if err != nil || c.closed {
		t.Fatal("full ledger failed the connection:", err)
	}
	_ = out // an ACK-only packet may still go
	if _, ok := s.sent[pn]; ok || len(s.sent) > maxSentPackets {
		t.Fatal("sent frames past a full ledger")
	}
}

// At capacity, entries for packets already declared lost, which recovery no
// longer tracks, are reclaimed, and the queued data goes.
func TestFullSentLedgerReclaimsLost(t *testing.T) {
	c := ledgerPair(t)
	s := &c.spaces[applicationSpace]
	// Packet numbers below the next one that recovery never tracked stand in
	// for packets declared lost.
	for pn := uint64(0); len(s.sent) < maxSentPackets; pn++ {
		if !c.recovery.Tracks(recovery.Application, pn) {
			s.sent[1<<40+pn] = &sendItem{frame: wire.PingFrame{}, transmitted: true}
		}
	}
	pn := s.nextPN
	if _, err := c.NextDatagram(time.Unix(200, 0)); err != nil || c.closed {
		t.Fatal(err)
	}
	if _, ok := s.sent[pn]; !ok {
		t.Fatal("queued data didn't go after reclaiming lost entries")
	}
	if len(s.sent) >= maxSentPackets {
		t.Fatal("lost entries not reclaimed:", len(s.sent))
	}
}

// A multipath path's full ledger makes it wait too.
func TestFullPathSentLedgerWaits(t *testing.T) {
	client, _ := sendingPathPair(t, wire.Version1)
	app := client.multipath.paths[1].application
	app.budget.Validate()
	app.congestion = openController{app.congestion}
	now := time.Unix(101, 0)
	s := app.space
	for len(s.sent) < maxSentPackets {
		pn := s.nextPN
		if err := app.sent.OnSent(recovery.Application, recovery.Packet{Number: pn, SentAt: now, Bytes: 1200, AckEliciting: true, InFlight: true}); err != nil {
			t.Fatal(err)
		}
		s.sent[pn] = &sendItem{frame: wire.PingFrame{}, transmitted: true}
		s.nextPN++
	}
	if err := client.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
		t.Fatal(err)
	}
	pn := s.nextPN
	if _, err := client.nextPacketOnPath(1, now); err != nil || client.closed {
		t.Fatal("full path ledger failed the connection:", err)
	}
	if _, ok := s.sent[pn]; ok {
		t.Fatal("sent frames past a full path ledger")
	}
}
