package transport

import (
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func TestMigrationRecoveryPreservesReliableLateACK(t *testing.T) {
	_, c := sendingPathPair(t, wire.Version1)
	c.config.allowIPMigration = true
	local := netip.MustParseAddrPort("192.0.2.1:443")
	oldRemote := netip.MustParseAddrPort("192.0.2.2:444")
	newRemote := netip.MustParseAddrPort("192.0.2.3:445")
	if err := c.bindPreparedPath(0, local, oldRemote); err != nil {
		t.Fatal(err)
	}
	app := c.multipath.paths[0].application
	now := time.Unix(101, 0)
	if err := c.queueFrameOnPath(0, wire.PingFrame{}); err != nil {
		t.Fatal(err)
	}
	pn := app.space.nextPN
	raw, err := c.nextPacketOnPath(0, now)
	if err != nil || raw == nil {
		t.Fatal(err)
	}
	item := app.space.sent[pn]
	beforeBytes := c.queuedBytes
	ledger, controller, timer, pacer := app.sent, app.congestion, app.timer, app.pacer
	app.sent.RTT.Update(25*time.Millisecond, 0, 0, true)
	app.congestion.OnACK(recovery.ACKEvent{Acked: []recovery.Packet{{Bytes: 1200, InFlight: true}}})
	app.probePending = true
	app.pacingAt = now.Add(time.Second)
	app.migration, _ = app.space.migrationSelection(*app.network)
	candidate, err := app.migration.observeAuthenticated(pathTuple{local, newRemote}, app.space.highestNonProbing+1, true, 1200, now, time.Second)
	if err != nil || candidate == nil {
		t.Fatal("candidate nomination failed", err)
	}
	candidate.validation.state = PathValidated
	candidate.budget.Validate()
	if committed, err := c.commitPeerRebinding(0, candidate); err != nil || !committed {
		t.Fatal("IP migration commit failed", err)
	}
	if app.network.remote != newRemote || c.CurrentRemoteAddr() != newRemote {
		t.Fatal("IP migration did not publish address")
	}
	if app.sent != ledger || app.congestion != controller || app.timer != timer || app.pacer != pacer {
		t.Fatal("reset broke path-zero aliases")
	}
	if app.sent.BytesInFlight != 0 || app.sent.RTT.Sampled || app.congestion.Window() != 12000 || !app.timer.Deadline().IsZero() || app.timer.Backoff() != 0 || app.probePending || !app.pacingAt.IsZero() {
		t.Fatal("reset retained old network evidence")
	}
	if app.space.nextPN != pn+1 || app.space.sent[pn] != item || !item.queued || c.queuedBytes != beforeBytes {
		t.Fatal("migration lost or recharged reliable item")
	}
	events, err := c.multipath.receiveReliableACK(wire.AckFrame{Ranges: []wire.AckRange{{Smallest: pn, Largest: pn}}}, now.Add(time.Second), 0, 0, false, func(item *sendItem) error { c.queuedBytes -= item.bytes; return nil })
	if err != nil || len(events.Acked) != 0 || !item.acked || app.sent.RTT.Sampled || app.congestion.Window() != 12000 {
		t.Fatal("late ACK lost completion or changed network estimates", err)
	}
	if c.queuedBytes != beforeBytes-item.bytes {
		t.Fatal("late completion charged incorrectly")
	}
}
