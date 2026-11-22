package transport

import (
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestMigrationRequiresEstablishedCIDUse(t *testing.T) {
	client, server := sendingPathPair(t, wire.Version1)
	ca := netip.MustParseAddrPort("192.0.2.1:443")
	sa := netip.MustParseAddrPort("192.0.2.2:444")
	changed := netip.MustParseAddrPort("192.0.2.1:445")
	client.bindPreparedPath(1, ca, sa)
	server.bindPreparedPath(1, sa, ca)
	client.multipath.paths[1].application.budget.Validate()
	app := server.multipath.paths[1].application
	app.validation.state = PathValidated
	server.config.allowCandidateMigration = true
	now := time.Unix(101, 0)
	send := func() []byte {
		t.Helper()
		client.queueFrameOnPath(1, wire.PingFrame{})
		b, err := client.nextPacketOnPath(1, now)
		now = now.Add(time.Millisecond)
		if err != nil || b == nil {
			t.Fatal(err)
		}
		return b
	}
	first := send()
	if err := server.handleBoundMultipathPacket(first, sa, ca, now); err != nil {
		t.Fatal(err)
	}
	peers := client.multipath.paths[1].peer
	found := false
	for seq := range peers.active {
		if seq != peers.current {
			peers.current = seq
			found = true
			break
		}
	}
	if !found {
		t.Fatal("missing spare CID")
	}
	fresh := send()
	if err := server.handleBoundMultipathPacket(fresh, sa, changed, now); err != nil {
		t.Fatal(err)
	}
	if app.migration != nil {
		t.Fatal("unused established CID admitted rebinding")
	}
	// Actual authenticated use on the established tuple grants that CID evidence.
	if err := server.handleBoundMultipathPacket(fresh, sa, ca, now); err != nil {
		t.Fatal(err)
	}
	candidate := send()
	if err := server.handleBoundMultipathPacket(candidate, sa, changed, now); err != nil {
		t.Fatal(err)
	}
	if app.migration == nil || app.migration.candidate == nil {
		t.Fatal("established CID reuse rejected")
	}
}

func TestLocalCIDObservationRetirement(t *testing.T) {
	ids := newLocalIDs([]byte("initial0"))
	frames, err := ids.issue(2)
	if err != nil || len(frames) != 1 {
		t.Fatal(err)
	}
	f := frames[0]
	ids.markSent(f.Sequence)
	tuple := pathTuple{netip.MustParseAddrPort("192.0.2.1:443"), netip.MustParseAddrPort("192.0.2.2:444")}
	ids.noteReceived(f.ConnectionID, tuple)
	if !ids.receivedOn(f.ConnectionID, tuple) {
		t.Fatal("missing observation")
	}
	if _, err := ids.retireWithType(f.Sequence, []byte("initial0"), 0x19); err != nil {
		t.Fatal(err)
	}
	if len(ids.observed) != 0 || ids.receivedOn(f.ConnectionID, tuple) {
		t.Fatal("retirement retained observation")
	}
}
