package transport

import (
	"bytes"
	"fmt"
	"io"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func TestMigrationCandidateProbeExchange(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, fresh := range []bool{false, true} {
			for _, ipChanged := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/fresh=%v/ip=%v", version, fresh, ipChanged), func(t *testing.T) {
					client, server := sendingPathPair(t, version)
					ca := netip.MustParseAddrPort("192.0.2.1:443")
					sa := netip.MustParseAddrPort("192.0.2.2:444")
					rebound := netip.MustParseAddrPort("192.0.2.1:445")
					if ipChanged {
						rebound = netip.MustParseAddrPort("192.0.2.3:445")
					}
					for _, id := range []uint64{0, 1} {
						if err := client.bindPreparedPath(id, ca, sa); err != nil {
							t.Fatal(err)
						}
						if err := server.bindPreparedPath(id, sa, ca); err != nil {
							t.Fatal(err)
						}
					}
					client.multipath.paths[1].application.validation.state = PathValidated
					client.multipath.paths[1].application.budget.Validate()
					app := server.multipath.paths[1].application
					app.validation.state = PathValidated
					server.config.allowCandidateMigration = true
					server.config.allowFreshCIDMigration = fresh
					server.config.allowIPMigration = ipChanged
					oldPeerSequence := server.multipath.paths[1].peer.current
					now := time.Unix(101, 0)
					client.queueFrameOnPath(1, wire.PingFrame{})
					established, err := client.nextPacketOnPath(1, now)
					if err != nil || established == nil {
						t.Fatal(err)
					}
					if err := server.handleBoundMultipathPacket(established, sa, ca, now); err != nil {
						t.Fatal(err)
					}
					// Drain the established ACK before testing candidate-only output.
					if _, err := server.nextPacketOnPath(1, now); err != nil {
						t.Fatal(err)
					}
					now = now.Add(time.Millisecond)
					if fresh {
						peers := client.multipath.paths[1].peer
						found := false
						for seq, peer := range peers.active {
							if seq != peers.current && !peer.used {
								peers.current = seq
								found = true
								break
							}
						}
						if !found {
							t.Fatal("missing fresh destination CID")
						}
					}
					client.queueFrameOnPath(1, wire.StreamFrame{StreamID: 0, Data: []byte("rebound"), Fin: true})
					ping, err := client.nextPacketOnPath(1, now)
					if err != nil || ping == nil {
						t.Fatal(err)
					}
					if err := server.handleBoundMultipathPacket(ping, sa, rebound, now); err != nil {
						t.Fatal(err)
					}
					candidate := app.migration.candidate
					pn, seals, oldCredit := app.space.nextPN, server.sealed[applicationSpace], app.budget.credit
					credit := candidate.budget.credit
					candidate.budget.credit = 0
					raw, _, err := server.nextMigrationProbe(1, now)
					if err != nil || raw != nil || app.space.nextPN != pn || server.sealed[applicationSpace] != seals || len(candidate.validation.probes) != 0 {
						t.Fatal("blocked probe consumed state", err)
					}
					if app.budget.credit != oldCredit {
						t.Fatal("blocked probe changed established budget")
					}
					oldSpace, oldSent, oldCongestion := app.space, app.sent, app.congestion
					candidate.budget.credit = credit
					app.sent.RTT.Update(25*time.Millisecond, 0, 0, true)
					var short, expanded bool
					extraPackets := uint64(0)
					if fresh {
						peers := server.multipath.paths[1].peer
						if !candidate.hasPeerSequence || peers.current != oldPeerSequence || candidate.peerSequence == peers.current {
							t.Fatal("reservation changed established sending CID")
						}
						app.budget.Validate()
						if err := server.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
							t.Fatal(err)
						}
						ordinary, err := server.nextPacketOnPath(1, now.Add(time.Millisecond))
						if err != nil || ordinary == nil {
							t.Fatal("ordinary output blocked by reservation", err)
						}
						oldCID := peers.active[oldPeerSequence].cid
						if !bytes.Equal(ordinary[1:1+len(oldCID)], oldCID) {
							t.Fatal("ordinary packet used candidate CID")
						}
						extraPackets++ // Simulate loss on the old tuple; retain its reliable PING.
					}
					for i := 0; i < 20 && candidate.validation.state != PathValidated; i++ {
						now = now.Add(10 * time.Millisecond)
						id, tuple, packet, err := server.nextBoundMultipathPacket(now)
						if err != nil {
							t.Fatal(err)
						}
						if packet != nil {
							if id != 1 || tuple != (pathTuple{sa, rebound}) {
								t.Fatal("probe used established tuple", id, tuple)
							}
							if fresh {
								cid := server.multipath.paths[1].peer.active[candidate.peerSequence].cid
								if !bytes.Equal(packet[1:1+len(cid)], cid) {
									t.Fatal("candidate probe used established CID")
								}
							}
							if len(packet) == 1200 {
								expanded = true
							} else {
								short = true
							}
							// Simulate a NAT translating the server's new remote tuple back to the
							// client's unchanged local socket. Packet numbers/keys remain shared.
							if err := client.handleBoundMultipathPacket(packet, ca, sa, now); err != nil {
								t.Fatal(err)
							}
						}
						response, err := client.nextPacketOnPath(1, now)
						if err != nil {
							t.Fatal(err)
						}
						if response != nil {
							if err := server.handleBoundMultipathPacket(response, sa, rebound, now); err != nil {
								t.Fatal(err)
							}
						}
					}
					if !short || !expanded || candidate.validation.state != PathValidated {
						t.Fatal("candidate did not complete address and MTU validation")
					}
					if app.space.nextPN != pn+2+extraPackets || server.sealed[applicationSpace] != seals+2+extraPackets {
						t.Fatal("probes did not share packet number/key usage domain")
					}
					if app.network.remote != rebound || !app.budget.Validated() || app.migration.candidate != nil {
						t.Fatal("validated port rebinding did not commit")
					}
					if fresh && server.multipath.paths[1].peer.current != candidate.peerSequence {
						t.Fatal("validated migration did not commit fresh CID")
					}
					if ipChanged && (app.sent.RTT.Sampled || app.sent.BytesInFlight != 0 || app.congestion.Window() != recovery.InitialWindow(1200)) {
						t.Fatal("IP change retained old network recovery")
					}
					if !ipChanged && !app.sent.RTT.Sampled {
						t.Fatal("port-only rebinding discarded RTT")
					}
					if app.space != oldSpace || app.sent != oldSent || app.congestion != oldCongestion {
						t.Fatal("port rebinding replaced packet/recovery history")
					}
					// The original stream packet was not ACKed or dispatched before admission.
					// Its unchanged ciphertext/PN can now be retransmitted and delivered.
					if _, err := server.AcceptStream(false); err == nil {
						t.Fatal("candidate stream delivered before admission")
					}
					if err := server.handleBoundMultipathPacket(ping, sa, rebound, now); err != nil {
						t.Fatal(err)
					}
					stream, err := server.AcceptStream(false)
					if err != nil {
						t.Fatal(err)
					}
					buffer := make([]byte, 32)
					n, err := server.ReadStream(stream, buffer)
					if err != io.EOF || string(buffer[:n]) != "rebound" {
						t.Fatal(n, err)
					}
					if !fresh && len(app.space.sent) != 0 {
						t.Fatal("challenge treated as reliably retransmitted data")
					}
				})
			}
		}
	}
}
