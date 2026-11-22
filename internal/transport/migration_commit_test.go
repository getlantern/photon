package transport

import (
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestPortRebindingPreservesOutstandingRecovery(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	local := netip.MustParseAddrPort("192.0.2.1:443")
	remote := netip.MustParseAddrPort("192.0.2.2:444")
	changed := netip.MustParseAddrPort("192.0.2.2:445")
	if err := c.bindPreparedPath(1, local, remote); err != nil {
		t.Fatal(err)
	}
	app := c.multipath.paths[1].application
	app.budget.Validate()
	now := time.Unix(101, 0)
	if err := c.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
		t.Fatal(err)
	}
	pn := app.space.nextPN
	if raw, err := c.nextPacketOnPath(1, now); err != nil || raw == nil {
		t.Fatal(err)
	}
	item := app.space.sent[pn]
	beforeFlight, ledger := app.sent.BytesInFlight, app.sent
	if err := c.queueFrameOnPath(1, wire.PathResponseFrame{}); err != nil {
		t.Fatal(err)
	}
	app.migration, _ = app.space.migrationSelection(*app.network)
	candidate, _ := app.migration.observeAuthenticated(pathTuple{local, changed}, 0, true, 1200, now, time.Second)
	candidate.validation.state = PathValidated
	candidate.budget.Validate()
	before := c.queuedBytes
	committed, err := c.commitPeerRebinding(1, candidate)
	if err != nil || !committed {
		t.Fatal(err)
	}
	if app.sent != ledger || app.sent.BytesInFlight != beforeFlight || app.space.sent[pn] != item || app.space.nextPN != pn+1 || c.queuedBytes != before-32 {
		t.Fatal("commit lost recovery or stale probe cleanup")
	}

	localSnapshot, remoteSnapshot, ok := c.CurrentPathAddresses(1)
	if !ok || localSnapshot != local || remoteSnapshot != changed {
		t.Fatal("secondary rebinding snapshot not committed")
	}
	if c.CurrentRemoteAddr().IsValid() {
		t.Fatal("secondary path replaced primary identity")
	}
	ack := wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: pn, Largest: pn}}}}
	if _, err := c.multipath.receiveReliableACK(ack, now.Add(time.Millisecond), 0, 25*time.Millisecond, true, func(item *sendItem) error { c.queuedBytes -= item.bytes; return nil }); err != nil {
		t.Fatal(err)
	}
	if !item.acked || app.sent.BytesInFlight != 0 {
		t.Fatal("old transmission could not complete after commit")
	}
	// A new IP needs its own congestion/RTT transition and remains uncommitted.
	other := pathTuple{local, netip.MustParseAddrPort("192.0.2.3:445")}
	next, _ := app.migration.observeAuthenticated(other, 1, true, 1200, now, time.Second)
	next.validation.state = PathValidated
	next.budget.Validate()
	committed, err = c.commitPeerRebinding(1, next)
	if err != nil || committed || app.network.remote != changed || app.migration.candidate != next {
		t.Fatal("IP change committed through port-only policy", err)
	}
}

func TestFreshCIDCommitRejectsPeerRetirement(t *testing.T) {
	_, c := sendingPathPair(t, wire.Version1)
	local := netip.MustParseAddrPort("192.0.2.1:443")
	remote := netip.MustParseAddrPort("192.0.2.2:444")
	changed := netip.MustParseAddrPort("192.0.2.2:445")
	if err := c.bindPreparedPath(1, local, remote); err != nil {
		t.Fatal(err)
	}
	app := c.multipath.paths[1].application
	app.migration, _ = app.space.migrationSelection(*app.network)
	candidate, _ := app.migration.observeAuthenticated(pathTuple{local, changed}, 0, true, 1200, time.Unix(101, 0), time.Second)
	peers := c.multipath.paths[1].peer
	candidate.freshCID = true
	candidate.peerSequence, candidate.hasPeerSequence = peers.reserveMigrationID(candidate)
	if !candidate.hasPeerSequence || !peers.sentMigrationID(candidate, candidate.peerSequence) {
		t.Fatal("reservation failed")
	}
	candidate.validation.state = PathValidated
	// Process a peer instruction that retires the candidate CID before commit.
	seq := candidate.peerSequence + 1
	replacement := peers.active[seq]
	if len(replacement.cid) == 0 {
		replacement.cid = []byte("newcid42")
	}
	if _, err := peers.add(wire.NewConnectionIDFrame{Sequence: seq, RetirePriorTo: seq, ConnectionID: replacement.cid, StatelessResetToken: replacement.token}); err != nil {
		t.Fatal(err)
	}
	if committed, err := c.commitPeerRebinding(1, candidate); err != nil || committed {
		t.Fatal("retired CID committed", committed, err)
	}
	if app.network.remote != remote || app.migration.candidate != nil {
		t.Fatal("stale proof changed address or stayed nominated")
	}
	_, snapshot, ok := c.CurrentPathAddresses(1)
	if !ok || snapshot != remote {
		t.Fatal("failed commit published address")
	}
}

func TestMigrationCommitRejectsSocketAddressChanges(t *testing.T) {
	for _, next := range []pathTuple{
		{netip.MustParseAddrPort("192.0.2.1:443"), netip.MustParseAddrPort("[2001:db8::2]:444")},
		{netip.MustParseAddrPort("192.0.2.3:443"), netip.MustParseAddrPort("192.0.2.2:444")},
	} {
		_, c := sendingPathPair(t, wire.Version1)
		c.config.allowIPMigration = true
		local := netip.MustParseAddrPort("192.0.2.1:443")
		remote := netip.MustParseAddrPort("192.0.2.2:444")
		if err := c.bindPreparedPath(1, local, remote); err != nil {
			t.Fatal(err)
		}
		app := c.multipath.paths[1].application
		app.migration, _ = app.space.migrationSelection(*app.network)
		candidate, err := app.migration.observeAuthenticated(next, 0, true, 1200, time.Unix(101, 0), time.Second)
		if err != nil || candidate == nil {
			t.Fatal(err)
		}
		candidate.validation.state = PathValidated
		app.sent.RTT.Update(time.Millisecond, 0, 0, true)
		if committed, err := c.commitPeerRebinding(1, candidate); err != nil || committed {
			t.Fatal("unsupported socket transition committed", err)
		}
		if *app.network != (pathTuple{local, remote}) || !app.sent.RTT.Sampled || app.migration.candidate != candidate {
			t.Fatal("rejected transition mutated established state")
		}
	}
}
