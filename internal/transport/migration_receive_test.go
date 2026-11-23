package transport

import (
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestAuthenticatedMigrationCandidateAdmission(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := sendingPathPair(t, version)
		ca := netip.MustParseAddrPort("192.0.2.1:443")
		sa := netip.MustParseAddrPort("192.0.2.2:444")
		changed := netip.MustParseAddrPort("192.0.2.1:445")
		client.bindPreparedPath(1, ca, sa)
		server.bindPreparedPath(1, sa, ca)
		app := server.multipath.paths[1].application
		app.validation.state = PathValidated
		client.multipath.paths[1].application.validation.state = PathValidated
		client.multipath.paths[1].application.budget.Validate()
		now := time.Unix(101, 0)
		send := func() []byte {
			t.Helper()
			if err := client.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
				t.Fatal(err)
			}
			b, err := client.nextPacketOnPath(1, now)
			now = now.Add(time.Millisecond)
			if err != nil || b == nil {
				t.Fatal(err)
			}
			return b
		}
		old := send()
		if err := server.handleBoundMultipathPacket(old, sa, ca, now); err != nil {
			t.Fatal(err)
		}
		newer := send()
		// The public/default gate must continue dropping changed tuples.
		if err := server.handleBoundMultipathPacket(newer, sa, changed, now); err != nil {
			t.Fatal(err)
		}
		if app.migration != nil {
			t.Fatal("default gate admitted migration")
		}
		server.config.allowCandidateMigration = true
		forged := append([]byte(nil), newer...)
		forged[len(forged)-1] ^= 1
		if err := server.handleBoundMultipathPacket(forged, sa, changed, now); err != nil {
			t.Fatal(err)
		}
		if app.migration != nil {
			t.Fatal("forgery allocated selection state")
		}
		if err := server.handleBoundMultipathPacket(old, sa, changed, now); err != nil {
			t.Fatal(err)
		}
		if app.migration != nil {
			t.Fatal("stale packet nominated address")
		}
		app.space.ackPending = false
		credit, expected := app.budget.credit, app.space.received.Expected()
		if err := server.handleBoundMultipathPacket(newer, sa, changed, now); err != nil {
			t.Fatal(err)
		}
		if app.migration == nil || app.migration.candidate == nil {
			t.Fatal("fresh authenticated address not nominated")
		}
		candidate := app.migration.candidate
		if candidate.budget.credit != uint64(3*len(newer)) || app.budget.credit != credit || app.space.ackPending || app.space.received.Expected() != expected || app.network.remote != ca {
			t.Fatal("candidate admission changed established credit/receipt/binding")
		}
		if err := server.handleBoundMultipathPacket(old, sa, ca, now); err != nil {
			t.Fatal(err)
		}
		if app.migration.candidate != candidate {
			t.Fatal("old established packet canceled candidate")
		}
		fresh := send()
		if err := server.handleBoundMultipathPacket(fresh, sa, ca, now); err != nil {
			t.Fatal(err)
		}
		if app.migration.candidate != nil {
			t.Fatal("newer established packet failed to cancel candidate")
		}
		next := send()
		if err := server.handleBoundMultipathPacket(next, sa, changed, now); err != nil {
			t.Fatal(err)
		}
		expiry := app.migration.candidate.validation.expires
		if err := server.expireMultipathPaths(expiry); err != nil {
			t.Fatal(err)
		}
		if app.migration.candidate != nil || app.network.remote != ca {
			t.Fatal("expiry lost established binding")
		}
		if err := server.handleBoundMultipathPacket(next, sa, changed, expiry); err != nil {
			t.Fatal(err)
		}
		if app.migration.candidate != nil {
			t.Fatal("replay revived expired candidate")
		}
	}
}

func TestClientRejectsUnknownServerAddress(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := sendingPathPair(t, version)
		ca := netip.MustParseAddrPort("192.0.2.1:443")
		sa := netip.MustParseAddrPort("192.0.2.2:444")
		changed := netip.MustParseAddrPort("192.0.2.2:445")
		client.bindPreparedPath(1, ca, sa)
		server.bindPreparedPath(1, sa, ca)
		app := client.multipath.paths[1].application
		app.validation.state = PathValidated
		serverApp := server.multipath.paths[1].application
		serverApp.validation.state = PathValidated
		serverApp.budget.Validate()
		// Even an accidentally enabled internal gate cannot allow server migration.
		client.config.allowCandidateMigration = true
		now := time.Unix(101, 0)
		for _, remote := range []netip.AddrPort{sa, changed} {
			if err := server.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
				t.Fatal(err)
			}
			packet, err := server.nextPacketOnPath(1, now)
			if err != nil || packet == nil {
				t.Fatalf("packet: %x, %v", packet, err)
			}
			if err := client.handleBoundMultipathPacket(packet, ca, remote, now); err != nil {
				t.Fatal(err)
			}
			now = now.Add(time.Millisecond)
		}
		if app.migration != nil || app.network.remote != sa {
			t.Fatal("client admitted unknown server address")
		}
	}
}
