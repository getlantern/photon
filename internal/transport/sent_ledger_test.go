package transport

import (
	"bytes"
	"net/netip"
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
	// Entries for packets declared lost, as queueLostItems records them.
	for pn := uint64(1 << 40); len(s.sent) < maxSentPackets; pn++ {
		s.sent[pn] = &sendItem{frame: wire.PingFrame{}, transmitted: true}
		noteUntracked(s, []recovery.Packet{{Number: pn}})
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

// With the ledger full of packets in flight and the peer silent, the PTO
// still sends its probe, from the headroom kept for probes, and the timer
// stays armed: recovery from a silent peer never waits on the full ledger.
func TestFullSentLedgerStillProbes(t *testing.T) {
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
	c.armTimer(now)
	for round := range 3 {
		deadline := c.RecoveryDeadline()
		if deadline.IsZero() {
			t.Fatal("no recovery deadline at round", round)
		}
		now = deadline
		if err := c.HandleTimeout(now); err != nil {
			t.Fatal(err)
		}
		start := s.nextPN
		for {
			out, err := c.NextDatagram(now)
			if err != nil || c.closed {
				t.Fatal(err)
			}
			if out == nil {
				break
			}
		}
		if s.nextPN == start {
			t.Fatal("no PTO probe at a full ledger, round", round)
		}
	}
}

// On an evacuated path, a packet declared lost isn't requeued, but a late
// ACK of it still completes its item: ACK processing, with real recovery,
// visits it among the untracked entries.
func TestEvacuatedPathLateACKCompletes(t *testing.T) {
	p, _, b := pathRecoveryPair(t)
	now := time.Unix(100, 0)
	items := make([]*sendItem, 4)
	for pn := range uint64(4) {
		sendPathPacket(t, b, now)
		items[pn] = &sendItem{frame: wire.PingFrame{}, transmitted: true}
		b.space.sent[pn] = items[pn]
	}
	b.evacuated = true
	completed := map[*sendItem]bool{}
	complete := func(item *sendItem) error { completed[item] = true; return nil }
	ack := func(lo, hi uint64, at time.Time) {
		t.Helper()
		events, err := p.receiveReliableACK(wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: lo, Largest: hi}}}}, at, 3, 0, false, complete)
		if err != nil {
			t.Fatal(err)
		}
		notePathLoss(b, events.Lost)
	}
	// Packets 1-3 acknowledged: packet 0 is lost by packet threshold.
	ack(1, 3, now.Add(10*time.Millisecond))
	if completed[items[0]] || len(b.space.queue) != 0 {
		t.Fatal("lost packet completed or requeued on an evacuated path")
	}
	ack(0, 3, now.Add(20*time.Millisecond))
	if !completed[items[0]] {
		t.Fatal("late ACK of a lost packet on an evacuated path didn't complete its item")
	}
}

// In-flight packets that never enter the frame ledger, such as DATAGRAMs,
// still count toward the limit: once recovery holds maxSentPackets of them,
// ordinary frames wait, leaving the headroom for the PTO probe.
func TestUnframedPacketsFillLedger(t *testing.T) {
	c := ledgerPair(t)
	s := &c.spaces[applicationSpace]
	now := time.Unix(200, 0)
	for range maxSentPackets {
		if err := c.recovery.OnSent(recovery.Application, recovery.Packet{Number: s.nextPN, SentAt: now, Bytes: 1, AckEliciting: true, InFlight: true}); err != nil {
			t.Fatal(err)
		}
		s.nextPN++
	}
	for {
		out, err := c.NextDatagram(now)
		if err != nil || c.closed {
			t.Fatal(err)
		}
		if out == nil {
			break
		}
	}
	if len(s.sent) != 0 {
		t.Fatalf("sent %d framed packets into the probe headroom", len(s.sent))
	}
	c.armTimer(now)
	now = c.RecoveryDeadline()
	if now.IsZero() {
		t.Fatal("no recovery deadline")
	}
	if err := c.HandleTimeout(now); err != nil {
		t.Fatal(err)
	}
	start := s.nextPN
	if _, err := c.NextDatagram(now); err != nil || c.closed {
		t.Fatal(err)
	}
	if s.nextPN == start {
		t.Fatal("no PTO probe")
	}
}

// The same on a multipath path: unframed packets fill its recovery, and an
// ordinary frame queued on the path waits.
func TestUnframedPacketsFillPathLedger(t *testing.T) {
	client, _ := sendingPathPair(t, wire.Version1)
	app := client.multipath.paths[1].application
	app.budget.Validate()
	app.congestion = openController{app.congestion}
	now := time.Unix(101, 0)
	s := app.space
	for range maxSentPackets {
		if err := app.sent.OnSent(recovery.Application, recovery.Packet{Number: s.nextPN, SentAt: now, Bytes: 1, AckEliciting: true, InFlight: true}); err != nil {
			t.Fatal(err)
		}
		s.nextPN++
	}
	if err := client.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.nextPacketOnPath(1, now); err != nil || client.closed {
		t.Fatal(err)
	}
	if len(s.sent) != 0 {
		t.Fatal("sent a framed packet into the path's probe headroom")
	}
}

// Migration PATH_CHALLENGEs are ordinary in-flight packets too: at a full
// ledger they wait rather than take the probe headroom.
func TestFullLedgerHoldsMigrationProbe(t *testing.T) {
	client, server := testPair(t, wire.Version1, streamParameters())
	drivePair(t, client, server, false, false)
	local := netip.MustParseAddrPort("192.0.2.1:443")
	next := netip.MustParseAddrPort("192.0.2.1:445")
	remote := netip.MustParseAddrPort("192.0.2.2:444")
	if err := client.activateBasePath(local, remote); err != nil {
		t.Fatal(err)
	}
	if err := server.activateBasePath(remote, local); err != nil {
		t.Fatal(err)
	}
	client.peerParams.DisableActiveMigration = false
	server.config.allowCandidateMigration = true
	server.config.allowFreshCIDMigration = true
	now := time.Unix(101, 0)
	if _, err := client.prepareLocalMigration(0, next, now); err != nil {
		t.Fatal(err)
	}
	app := client.multipath.paths[0].application
	app.congestion = openController{app.congestion}
	for range maxSentPackets {
		if err := app.sent.OnSent(recovery.Application, recovery.Packet{Number: app.space.nextPN, SentAt: now, Bytes: 1, AckEliciting: true, InFlight: true}); err != nil {
			t.Fatal(err)
		}
		app.space.nextPN++
	}
	pn := app.space.nextPN
	if packet, _, err := client.nextLocalMigrationProbe(0, now); err != nil || packet != nil || app.space.nextPN != pn {
		t.Fatal("migration probe sent into the probe headroom", err)
	}
}

// A queued PATH_RESPONSE outranks a pending ACK, but when the full ledger
// holds the response, the ACK, which needs no ledger entry, still goes.
func TestFullPathLedgerStillACKs(t *testing.T) {
	client, server := sendingPathPair(t, wire.Version1)
	app := client.multipath.paths[1].application
	app.budget.Validate()
	server.multipath.paths[1].application.budget.Validate()
	app.congestion = openController{app.congestion}
	now := time.Unix(101, 0)
	if err := server.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
		t.Fatal(err)
	}
	s := app.space
	for range 20 {
		b, err := server.nextPacketOnPath(1, now)
		if err != nil {
			t.Fatal(err)
		}
		if b != nil {
			if err := app.budget.Received(len(b)); err != nil {
				t.Fatal(err)
			}
			if err := client.handleMultipathPacket(b, now); err != nil {
				t.Fatal(err)
			}
		}
		if s.ackPending || !s.ackDueAt.IsZero() {
			break
		}
		now = now.Add(time.Millisecond)
	}
	if !s.ackPending && s.ackDueAt.IsZero() {
		t.Fatal("the server's packets left no ACK due")
	}
	if !s.ackPending {
		now = s.ackDueAt
	}
	for len(s.sent) < maxSentPackets {
		pn := s.nextPN
		if err := app.sent.OnSent(recovery.Application, recovery.Packet{Number: pn, SentAt: now, Bytes: 1200, AckEliciting: true, InFlight: true}); err != nil {
			t.Fatal(err)
		}
		s.sent[pn] = &sendItem{frame: wire.PingFrame{}, transmitted: true}
		s.nextPN++
	}
	if err := client.queueFrameOnPath(1, wire.PathResponseFrame{Data: [8]byte{1}}); err != nil {
		t.Fatal(err)
	}
	if !app.queuedPathResponse() {
		t.Fatal("PATH_RESPONSE not queued")
	}
	b, err := client.nextPacketOnPath(1, now)
	if err != nil || client.closed {
		t.Fatal(err)
	}
	if b == nil || s.ackPending {
		t.Fatal("full ledger held the ACK behind a queued PATH_RESPONSE")
	}
	if !app.queuedPathResponse() {
		t.Fatal("PATH_RESPONSE left the queue without a ledger entry")
	}
}
