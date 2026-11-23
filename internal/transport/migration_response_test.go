package transport

import (
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestMigrationResponseAcrossTuples(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, base := range []bool{false, true} {
			for _, localMigration := range []bool{false, true} {
				for _, established := range []bool{false, true} {
					t.Run(fmt.Sprintf("%x/base=%v/local=%v/established=%v", version, base, localMigration, established), func(t *testing.T) {
						client, server := testPair(t, version, streamParameters())
						drivePair(t, client, server, false, false)
						old := netip.MustParseAddrPort("192.0.2.1:443")
						next := netip.MustParseAddrPort("192.0.2.1:445")
						other := netip.MustParseAddrPort("192.0.2.1:446")
						remote := netip.MustParseAddrPort("192.0.2.2:444")
						if err := client.activateBasePath(old, remote); err != nil {
							t.Fatal(err)
						}
						if err := server.activateBasePath(remote, old); err != nil {
							t.Fatal(err)
						}
						client.multipath.baseOnly, server.multipath.baseOnly = base, base
						client.peerParams.DisableActiveMigration = false
						server.config.allowCandidateMigration = true
						now := time.Unix(101, 0)
						var candidate *migrationCandidate
						receiver, sender := server, client
						arrival := pathTuple{remote, other}
						if established {
							arrival.remote = old
						}
						if localMigration {
							receiver, sender = client, server
							arrival = pathTuple{other, remote}
							if established {
								arrival.local = old
							}
							var err error
							candidate, err = client.prepareLocalMigration(0, next, now)
							if err != nil {
								t.Fatal(err)
							}
							raw, _, err := client.nextLocalMigrationProbe(0, now)
							if err != nil || raw == nil {
								t.Fatal("no client probe", err)
							}
						} else {
							app := server.multipath.paths[0].application
							var err error
							app.migration, err = app.space.migrationSelection(*app.network)
							if err != nil {
								t.Fatal(err)
							}
							candidate, err = app.migration.observeAuthenticated(pathTuple{remote, next}, app.space.highestNonProbing+1, true, 1200, now, time.Second)
							if err != nil {
								t.Fatal(err)
							}
							raw, _, err := server.nextMigrationProbe(0, now)
							if err != nil || raw == nil {
								t.Fatal("no server probe", err)
							}
						}
						token := candidate.validation.probes[0].token
						pn := sender.spaces[applicationSpace].nextPN + 10
						seal := func(token [8]byte, number uint64, withStream bool) []byte {
							payload, _ := (wire.PathResponseFrame{Data: token}).Append(nil)
							if withStream {
								streamID := uint64(0)
								if sender.config.Server {
									streamID = 1
								}
								payload, _ = (wire.StreamFrame{StreamID: streamID, Data: []byte("withheld"), Fin: true}).Append(payload)
							}
							return sealPathPayload(t, sender.writeKeys(applicationSpace), receiver.config.LocalCID, 0, number, payload)
						}
						forged := seal(token, pn, false)
						forged[len(forged)-1] ^= 1
						if err := receiver.handleBoundMultipathPacket(forged, arrival.local, arrival.remote, now); err != nil {
							t.Fatal(err)
						}
						wrong := token
						wrong[0] ^= 1
						if err := receiver.handleBoundMultipathPacket(seal(wrong, pn, false), arrival.local, arrival.remote, now); err != nil {
							t.Fatal(err)
						}
						if candidate.validation.state != PathValidating {
							t.Fatal("invalid cross-tuple proof admitted")
						}
						if err := receiver.handleBoundMultipathPacket(seal(token, pn+1, !established), arrival.local, arrival.remote, now); err != nil {
							t.Fatal(err)
						}
						if candidate.validation.state != PathValidated {
							t.Fatal("cross-tuple proof rejected")
						}
						app := receiver.multipath.paths[0].application
						if localMigration {
							if app.network.local != old || app.localMigration != candidate {
								t.Fatal("proof bypassed owner commit")
							}
							if err := client.commitLocalMigration(0, candidate); err != nil {
								t.Fatal(err)
							}
							if app.network.local != next {
								t.Fatal("wrong local tuple committed")
							}
						} else if app.network.remote != next {
							t.Fatal("arrival address replaced probed address")
						}
						if !established {
							if _, err := receiver.AcceptStream(false); err == nil {
								t.Fatal("unvalidated arrival tuple delivered application data")
							}
						}
					})
				}
			}
		}
	}
}
