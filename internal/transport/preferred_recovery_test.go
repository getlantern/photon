package transport

import (
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// Exercise retransmission deadlines and delayed authenticated proofs with the
// real packet protection layer. The deterministic network withholds probes or
// responses; independent-peer impaired-network coverage is a separate gate.
func TestPreferredValidationRecovery(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, mode := range []string{"lost-challenges", "delayed-first-response", "response-after-expiry"} {
			t.Run(fmt.Sprintf("%x/%s", version, mode), func(t *testing.T) {
				client, server, address := preferredAddressPair(t, version)
				local := netip.MustParseAddrPort("127.0.0.1:4000")
				original := netip.MustParseAddrPort("127.0.0.1:443")
				preferred := netip.AddrPortFrom(netip.AddrFrom4(address.IPv4), address.IPv4Port)
				if err := client.activateBasePath(local, original); err != nil {
					t.Fatal(err)
				}
				now := time.Unix(101, 0)
				candidate, err := client.preparePreferredMigration(local, preferred, now)
				if err != nil {
					t.Fatal(err)
				}
				app := client.multipath.paths[0].application
				for attempt := 0; attempt < 3; attempt++ {
					if attempt > 0 {
						now = candidate.validation.next
					}
					raw, tuple, err := client.nextLocalMigrationProbe(0, now)
					if err != nil || len(raw) != 1200 || tuple.remote != preferred {
						t.Fatalf("probe %d: %d %v %v", attempt, len(raw), tuple, err)
					}
					if len(candidate.validation.probes) != attempt+1 {
						t.Fatal("probe not recorded")
					}
					// Original-path packets continue to authenticate while validation waits.
					ping := makePeerPacket(t, server, applicationSpace, uint64(1000+attempt), wire.PingFrame{})
					if err := client.handleBoundMultipathPacket(ping, local, original, now); err != nil {
						t.Fatal(err)
					}
					if app.space.received.Expected() != uint64(1001+attempt) {
						t.Fatal("original-path packet was not accepted")
					}
					if app.network.remote != original || client.peerIDs.current != 0 {
						t.Fatal("probe or original traffic switched destination")
					}
				}
				index := 2
				if mode != "lost-challenges" {
					index = 0
				}
				delayedToken := candidate.validation.probes[1].token
				response := makePeerPacket(t, server, applicationSpace, 2000, wire.PathResponseFrame{Data: candidate.validation.probes[index].token})
				if mode == "response-after-expiry" {
					now = candidate.validation.expires
					if _, _, err := client.nextLocalMigrationProbe(0, now); err != nil {
						t.Fatal(err)
					}
				}
				if err := client.handleBoundMultipathPacket(response, local, preferred, now); err != nil {
					t.Fatal(err)
				}
				if mode == "response-after-expiry" {
					if app.localMigration != nil || app.network.remote != original || client.peerIDs.current != 0 {
						t.Fatal("late proof revived expired migration")
					}
					return
				}
				if candidate.validation.state != PathValidated {
					t.Fatal("retained probe response failed validation")
				}
				if err := client.commitLocalMigration(0, candidate); err != nil {
					t.Fatal(err)
				}
				if app.network.remote != preferred {
					t.Fatal("validated destination not committed")
				}
				// A later response for another probe must not switch back or resurrect state.
				duplicate := makePeerPacket(t, server, applicationSpace, 2001, wire.PathResponseFrame{Data: delayedToken})
				if err := client.handleBoundMultipathPacket(duplicate, local, preferred, now); err != nil {
					t.Fatal(err)
				}
				if app.localMigration != nil || app.network.remote != preferred {
					t.Fatal("reordered proof changed committed state")
				}
			})
		}
	}
}
