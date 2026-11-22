package transport

import (
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// One local migration owns path zero at a time. Automatic preferred migration
// defers to an existing application move; application requests report busy while
// preferred validation owns the slot. Neither may steal the other's reserved CID.
func TestPreferredAndLocalMigrationSerialization(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, preferredFirst := range []bool{false, true} {
			for _, sent := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/preferred-first=%v/sent=%v", version, preferredFirst, sent), func(t *testing.T) {
					client, server, address := preferredAddressPair(t, version)
					local := netip.MustParseAddrPort("127.0.0.1:4000")
					nextLocal := netip.MustParseAddrPort("127.0.0.1:4001")
					original := netip.MustParseAddrPort("127.0.0.1:443")
					preferred := netip.AddrPortFrom(netip.AddrFrom4(address.IPv4), address.IPv4Port)
					client.peerParams.DisableActiveMigration = false
					if err := client.activateBasePath(local, original); err != nil {
						t.Fatal(err)
					}
					endpoint := &ServerEndpoint{locals: map[netip.AddrPort]bool{local: true, nextLocal: true}}
					now := time.Unix(101, 0)
					app := client.multipath.paths[0].application
					var first *migrationCandidate
					var err error
					if preferredFirst {
						if err := endpoint.startPreferredMigration(client, now); err != nil {
							t.Fatal(err)
						}
						first = app.localMigration
						if first == nil || !first.preferred {
							t.Fatal("preferred migration not started")
						}
						if candidate, err := client.prepareLocalMigration(0, nextLocal, now); candidate != nil || err == nil {
							t.Fatal("local move stole preferred slot")
						}
					} else {
						first, err = client.prepareLocalMigration(0, nextLocal, now)
						if err != nil {
							t.Fatal(err)
						}
						if err := endpoint.startPreferredMigration(client, now); err != nil {
							t.Fatal(err)
						}
						if client.preferredAttempted {
							t.Fatal("deferred preferred address marked attempted")
						}
					}
					if app.localMigration != first || app.network.local != local || app.network.remote != original {
						t.Fatal("competing request mutated migration")
					}
					reservations := 0
					for _, cid := range client.peerIDs.active {
						if cid.reservation != nil {
							reservations++
							if cid.reservation != first {
								t.Fatal("competing reservation")
							}
						}
					}
					if reservations != 1 {
						t.Fatal("reservation count", reservations)
					}
					if sent {
						raw, _, err := client.nextLocalMigrationProbe(0, now)
						if err != nil || raw == nil {
							t.Fatal("probe before cancellation", err)
						}
					}
					if err := client.cancelLocalMigration(0, first); err != nil {
						t.Fatal(err)
					}
					if sent {
						if _, ok := client.peerIDs.active[first.peerSequence]; ok {
							t.Fatal("used CID retained after cancellation")
						}
						if !preferredFirst {
							if err := endpoint.startPreferredMigration(client, now); err != nil {
								t.Fatal(err)
							}
							if app.localMigration != nil || client.preferredAttempted {
								t.Fatal("CID exhaustion consumed deferred attempt")
							}
						}
						replacement := makePeerPacket(t, server, applicationSpace, 2000, wire.NewConnectionIDFrame{Sequence: 2, ConnectionID: []byte("replace2"), StatelessResetToken: [16]byte{73}})
						if err := client.handleBoundMultipathPacket(replacement, local, original, now); err != nil {
							t.Fatal(err)
						}
					}
					if preferredFirst {
						second, err := client.prepareLocalMigration(0, nextLocal, now)
						if err != nil || second == nil || second.preferred || second.tuple.remote != original {
							t.Fatal("local move after cancellation", err)
						}
					} else {
						if err := endpoint.startPreferredMigration(client, now); err != nil {
							t.Fatal(err)
						}
						second := app.localMigration
						if second == nil || !second.preferred || second.tuple.remote != preferred || !client.preferredAttempted {
							t.Fatal("preferred attempt not resumed")
						}
					}
				})
			}
		}
	}
}
