package transport

import (
	"errors"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestLocalMigrationMutualValidationBeforeCommit(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, base := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/base=%v", version, base), func(t *testing.T) {
				client, server := testPair(t, version, streamParameters())
				drivePair(t, client, server, false, false)
				old := netip.MustParseAddrPort("192.0.2.1:443")
				next := netip.MustParseAddrPort("192.0.2.1:445")
				remote := netip.MustParseAddrPort("192.0.2.2:444")
				if err := client.activateBasePath(old, remote); err != nil {
					t.Fatal(err)
				}
				if err := server.activateBasePath(remote, old); err != nil {
					t.Fatal(err)
				}
				client.multipath.baseOnly, server.multipath.baseOnly = base, base
				client.peerParams.DisableActiveMigration = false
				server.config.allowCandidateMigration, server.config.allowFreshCIDMigration = true, true
				now := time.Unix(101, 0)
				candidate, err := client.prepareLocalMigration(0, next, now)
				if err != nil {
					t.Fatal(err)
				}
				app := client.multipath.paths[0].application
				expected := app.space.received.Expected()
				app.space.ackPending = false
				testedProof := false
				testedBlockedResponse := false
				for range 12 {
					raw, tuple, err := client.nextLocalMigrationProbe(0, now)
					if err != nil {
						t.Fatal(err)
					}
					if raw != nil {
						if tuple != (pathTuple{next, remote}) {
							t.Fatal("client proof used old local tuple")
						}
						if err := server.handleBoundMultipathPacket(raw, remote, next, now); err != nil {
							t.Fatal(err)
						}
					}
					response, tuple, err := server.nextMigrationProbe(0, now)
					if err != nil {
						t.Fatal(err)
					}
					if response != nil {
						if tuple != (pathTuple{remote, next}) {
							t.Fatal("server response used established address")
						}
						if !testedProof {
							forged := append([]byte(nil), response...)
							forged[len(forged)-1] ^= 1
							if err := client.handleBoundMultipathPacket(forged, next, remote, now); err != nil {
								t.Fatal(err)
							}
							wrong := netip.MustParseAddrPort("192.0.2.3:444")
							if err := client.handleBoundMultipathPacket(response, next, wrong, now); err != nil {
								t.Fatal(err)
							}
							if candidate.validation.state == PathValidated {
								t.Fatal("forged/unknown-server proof admitted")
							}
							testedProof = true
						}
						if err := client.handleBoundMultipathPacket(response, next, remote, now); err != nil {
							t.Fatal(err)
						}
						if candidate.responsePending != nil && !testedBlockedResponse {
							pending, pn := candidate.responsePending, app.space.nextPN
							tail, charge, received := candidate.responseTail, client.queuedBytes, app.space.received.Expected()
							for replay := 0; replay < 16; replay++ {
								if err := client.handleBoundMultipathPacket(response, next, remote, now); err != nil {
									t.Fatal("local candidate replay", err)
								}
							}
							if candidate.responsePending != pending || candidate.responseTail != tail || client.queuedBytes != charge || app.space.received.Expected() != received || app.space.ackPending {
								t.Fatal("local replay changed response/admission state")
							}

							flight := app.sent.BytesInFlight
							app.sent.BytesInFlight = app.congestion.Window()
							blocked, _, err := client.nextLocalMigrationProbe(0, now)
							app.sent.BytesInFlight = flight
							if err != nil || blocked != nil || candidate.responsePending != pending || app.space.nextPN != pn {
								t.Fatal("blocked response consumed pending work", err)
							}
							testedBlockedResponse = true
						}
					}
					now = now.Add(100 * time.Millisecond)
				}
				if !testedProof || !testedBlockedResponse || candidate.validation.state != PathValidated || server.multipath.paths[0].application.network.remote != next {
					t.Fatal("mutual proof exchange did not validate")
				}
				if app.network.local != old || app.localMigration != candidate || app.space.received.Expected() != expected || app.space.ackPending {
					t.Fatal("candidate receive crossed local admission boundary")
				}
				payload, _ := (wire.StreamFrame{StreamID: 1, Data: []byte("withheld"), Fin: true}).Append(nil)
				raw := sealPathPayload(t, server.writeKeys(applicationSpace), server.peerCID, 0, server.spaces[applicationSpace].nextPN+10, payload)
				if err := client.handleBoundMultipathPacket(raw, next, remote, now); err != nil {
					t.Fatal(err)
				}
				if _, err := client.AcceptStream(false); err == nil || app.space.received.Expected() != expected || app.space.ackPending {
					t.Fatal("candidate application data admitted before local commit")
				}
				candidate.responsePending = &wire.PathResponseFrame{Data: [8]byte{1}}
				if err := candidate.queueResponse(wire.PathResponseFrame{Data: [8]byte{2}}); err != nil {
					t.Fatal(err)
				}
				if err := candidate.queueResponse(wire.PathResponseFrame{Data: [8]byte{3}}); err != nil {
					t.Fatal(err)
				}
				candidate.pacingAt = now.Add(time.Millisecond)
				if got := client.multipathPathsDeadline(); got != candidate.pacingAt {
					t.Fatalf("validated candidate lost response pacing deadline: %v", got)
				}
				beforeBytes, pn, cid := client.queuedBytes, app.space.nextPN, client.multipath.paths[0].peer.current
				client.queuedBytes = maxSendBytes
				app.sent.BytesInFlight = app.congestion.Window()
				app.sent.RTT.Update(25*time.Millisecond, 0, 0, true)
				if err := client.commitLocalMigration(0, candidate); err != nil {
					t.Fatal(err)
				}
				if app.controlResponses.count != 2 || app.controlResponses.frames[0].Data != ([8]byte{2}) || app.controlResponses.frames[1].Data != ([8]byte{3}) || candidate.responseTail.count != 0 {
					t.Fatal("commit lost response tail")
				}
				if client.queuedBytes != maxSendBytes || app.migrationResponse == nil {
					t.Fatal("saturated commit lost response or changed queue charge")
				}
				if app.sent.BytesInFlight != 0 || app.sent.RTT.Sampled || app.space.nextPN != pn || app.congestion.Window() != 12000 || candidate.responsePending != nil {
					t.Fatal("local commit retained old recovery state or lost pending response")
				}
				app.sent.BytesInFlight = app.congestion.Window()
				pendingResponse := app.migrationResponse
				blocked, err := client.nextPacketOnPath(0, now)
				if err != nil || blocked != nil || app.migrationResponse != pendingResponse || app.space.nextPN != pn {
					t.Fatal("blocked committed response consumed work", err)
				}
				app.sent.BytesInFlight = 0
				app.space.ackPending = true
				_, responseTuple, responsePacket, err := client.nextBoundMultipathPacket(now)
				if err != nil || responsePacket == nil || responseTuple.local != next || app.migrationResponse != nil || client.queuedBytes != maxSendBytes || !app.space.ackPending {
					t.Fatal("response blocked by saturated application queue", err)
				}
				decoded, err := server.authenticateMultipathPacket(responsePacket, now)
				if err != nil || decoded == nil {
					t.Fatal("invalid committed response", err)
				}
				response, ok := decoded.packet.frames[0].(wire.PathResponseFrame)
				if !ok || response.Data != ([8]byte{1}) {
					t.Fatal("pending response not sent first")
				}
				client.queuedBytes = beforeBytes
				localSnapshot, remoteSnapshot, ok := client.CurrentPathAddresses(0)
				if !ok || localSnapshot != next || remoteSnapshot != remote || app.network.local != next || app.localMigration != nil || client.multipath.paths[0].peer.current != candidate.peerSequence {
					t.Fatal("local commit did not switch ownership and snapshots")
				}

				if err := client.handleBoundMultipathPacket(raw, next, remote, now); err != nil {
					t.Fatal(err)
				}
				if _, err := client.AcceptStream(false); err != nil {
					t.Fatal("committed tuple did not deliver stream", err)
				}
				if err := client.commitLocalMigration(0, candidate); !errors.Is(err, errMigrationState) {
					t.Fatal("stale commit accepted", err)
				}
				_, outputTuple, output, err := client.nextBoundMultipathPacket(now)
				if err != nil || output == nil || outputTuple.local != next {
					t.Fatal("committed output used wrong tuple", err)
				}
				if _, exists := client.multipath.paths[0].peer.active[cid]; exists {
					t.Fatal("old CID not retired by client output")
				}
			})
		}
	}
}
