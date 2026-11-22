package transport

import (
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestPreferredServerAdmissionAndCommit(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			client, server, address := preferredAddressPair(t, version)
			local := netip.MustParseAddrPort("127.0.0.1:4000")
			original := netip.MustParseAddrPort("127.0.0.1:443")
			preferred := netip.AddrPortFrom(netip.AddrFrom4(address.IPv4), address.IPv4Port)
			if err := client.activateBasePath(local, original); err != nil {
				t.Fatal(err)
			}
			if err := server.activateBasePath(original, local); err != nil {
				t.Fatal(err)
			}
			server.config.allowCandidateMigration, server.config.allowFreshCIDMigration = true, true
			now := time.Unix(101, 0)
			if _, err := client.preparePreferredMigration(local, preferred, now); err != nil {
				t.Fatal(err)
			}
			raw, _, err := client.nextLocalMigrationProbe(0, now)
			if err != nil || raw == nil {
				t.Fatal(err)
			}
			app := server.multipath.paths[0].application
			// Authentication and local-address authorization precede allocation.
			forged := append([]byte(nil), raw...)
			forged[len(forged)-1] ^= 1
			if err := server.handleBoundMultipathPacket(forged, preferred, local, now); err != nil {
				t.Fatal(err)
			}
			unknown := netip.MustParseAddrPort("127.0.0.1:8444")
			if err := server.handleBoundMultipathPacket(raw, unknown, local, now); err != nil {
				t.Fatal(err)
			}
			if app.migration != nil {
				t.Fatal("unauthorized packet allocated candidate")
			}
			if err := server.handleBoundMultipathPacket(raw, preferred, local, now); err != nil {
				t.Fatal(err)
			}
			if app.migration == nil || app.migration.candidate == nil {
				t.Fatal("probing-only preferred packet ignored")
			}
			candidate := app.migration.candidate
			if !candidate.preferred || !candidate.freshCID || app.network.local != original {
				t.Fatal("preferred admission state")
			}
			// Newer established-path traffic must preserve preferred validation.
			packet := makePeerPacket(t, client, applicationSpace, 1000, wire.PingFrame{})
			if err := server.handleBoundMultipathPacket(packet, original, local, now); err != nil {
				t.Fatal(err)
			}
			if app.migration.candidate != candidate {
				t.Fatal("original traffic erased preferred probe")
			}
			// First send the owed response, then the server's own reachability challenge.
			for i := 0; i < 8 && len(candidate.validation.probes) == 0; i++ {
				output, tuple, err := server.nextMigrationProbe(0, now.Add(time.Duration(i)*20*time.Millisecond))
				if err != nil {
					t.Fatal(err)
				}
				if output != nil && tuple != (pathTuple{preferred, local}) {
					t.Fatal("probe used wrong local socket")
				}
			}
			if len(candidate.validation.probes) == 0 || !candidate.hasPeerSequence {
				t.Fatal("server did not validate using a fresh CID")
			}
			token := candidate.validation.probes[0].token
			proof := makePeerPacket(t, client, applicationSpace, 1001, wire.PathResponseFrame{Data: token})
			if err := server.handleBoundMultipathPacket(proof, preferred, local, now.Add(200*time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			if candidate.validation.state != PathValidated || app.network.local != original || app.migration.canCommit(candidate) {
				t.Fatal("proof alone switched server")
			}
			nomination := makePeerPacket(t, client, applicationSpace, 1002, wire.PingFrame{})
			if err := server.handleBoundMultipathPacket(nomination, preferred, local, now.Add(220*time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			if app.network.local != preferred || app.network.remote != local || app.migration.candidate != nil || server.peerIDs.current != candidate.peerSequence {
				t.Fatal("validated preferred nomination did not commit")
			}
		})
	}
}
