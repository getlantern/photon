package transport

import (
	"errors"
	"fmt"
	"io"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

func TestLocalMigrationValidationPacketLoss(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, base := range []bool{false, true} {
			for _, loss := range []string{"client-challenge", "server-challenge", "client-response", "server-response", "all-server-response", "client-stream", "delay-client-response", "delay-server-response", "delay-client-stream", "late-client-challenge", "late-server-response"} {
				t.Run(fmt.Sprintf("%x/base=%v/%s", version, base, loss), func(t *testing.T) {
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
					// The simulated driver bypasses newNetworkOwner, which normally
					// publishes the initial peer address before path activation.
					server.publishRemoteAddress(old)
					client.multipath.baseOnly, server.multipath.baseOnly = base, base
					client.peerParams.DisableActiveMigration = false
					server.config.allowCandidateMigration, server.config.allowFreshCIDMigration = true, true
					now := time.Unix(101, 0)
					candidate, err := client.prepareLocalMigration(0, next, now)
					if err != nil {
						t.Fatal(err)
					}
					id, err := client.OpenStream(false)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := client.WriteStream(id, []byte("survives validation loss"), true); err != nil {
						t.Fatal(err)
					}
					dropped := false
					accepted, finished := false, false
					var received []byte
					var held []byte
					var heldTuple pathTuple
					var heldReceiver *Conn
					var heldNumber uint64
					var releaseAt time.Time
					released := false
					for tick := 0; tick < 80 && (!finished || held != nil); tick++ {
						if held != nil && !now.Before(releaseAt) {
							if heldReceiver.spaces[applicationSpace].received.Expected() <= heldNumber {
								t.Fatal("delayed packet did not arrive after newer admitted packets")
							}
							// Deliver twice to check both reordering and duplicate handling.
							for range 2 {
								if err := heldReceiver.handleBoundMultipathPacket(held, heldTuple.remote, heldTuple.local, now); err != nil {
									t.Fatal(err)
								}
							}
							held = nil
							released = true
						}
						for _, c := range []*Conn{client, server} {
							if deadline := c.RecoveryDeadline(); !deadline.IsZero() && !now.Before(deadline) {
								if err := c.HandleTimeout(now); err != nil {
									t.Fatal(err)
								}
							}
						}
						for batch := 0; batch < 16; batch++ {
							progress := false
							for side, sender := range []*Conn{client, server} {
								receiver := server
								label := "client"
								if side == 1 {
									receiver = client
									label = "server"
								}
								_, tuple, raw, err := sender.nextBoundMultipathPacket(now)
								if err != nil {
									t.Fatal(err)
								}
								if raw == nil {
									continue
								}
								progress = true
								if !dropped || strings.HasPrefix(loss, "all-") || strings.HasPrefix(loss, "late-") {
									dropThis := false
									target := strings.TrimPrefix(strings.TrimPrefix(strings.TrimPrefix(loss, "all-"), "delay-"), "late-")
									header, err := wire.ParseHeader(raw, len(receiver.config.LocalCID))
									if err != nil {
										t.Fatal(err)
									}
									_, payload, number, err := receiver.readKeys(applicationSpace).Open(raw, header.PNOffset, receiver.spaces[applicationSpace].received.Expected())
									if err != nil {
										t.Fatal(err)
									}
									frames, _, err := parsePacketFrames(payload, wire.FrameContext{PacketType: wire.OneRTT, SenderIsServer: side == 1, MultipathNegotiated: !base})
									if err != nil {
										t.Fatal(err)
									}
									for _, frame := range frames {
										switch f := frame.(type) {
										case wire.StreamFrame:
											if target == label+"-stream" && len(f.Data) > 0 {
												dropThis = true
											}
										case wire.PathChallengeFrame:
											if target == label+"-challenge" {
												dropThis = true
											}
										case wire.PathResponseFrame:
											if target == label+"-response" {
												dropThis = true
											}
										}
									}
									if dropThis {
										dropped = true
										if (strings.HasPrefix(loss, "delay-") || strings.HasPrefix(loss, "late-")) && held == nil && !released {
											held = append([]byte(nil), raw...)
											heldTuple, heldReceiver, heldNumber = tuple, receiver, number
											releaseAt = now.Add(1500 * time.Millisecond)
											if strings.HasPrefix(loss, "late-") {
												releaseAt = now.Add(4500 * time.Millisecond)
											}
										}
										continue
									}
								}
								if err := receiver.handleBoundMultipathPacket(raw, tuple.remote, tuple.local, now); err != nil {
									t.Fatal(err)
								}
								if !candidate.committed && candidate.validation.state == PathValidated {
									if err := client.commitLocalMigration(0, candidate); err != nil {
										t.Fatal(err)
									}
								}
							}
							if !progress {
								break
							}
						}
						if !accepted {
							got, err := server.AcceptStream(false)
							if err == nil {
								if got != id {
									t.Fatal("wrong stream")
								}
								accepted = true
							} else if !errors.Is(err, stream.ErrWouldBlock) {
								t.Fatal(err)
							}
						}
						if accepted {
							var buf [64]byte
							n, err := server.ReadStream(id, buf[:])
							received = append(received, buf[:n]...)
							if err == io.EOF {
								finished = true
							} else if err != nil && !errors.Is(err, stream.ErrWouldBlock) {
								t.Fatal(err)
							}
						}
						now = now.Add(100 * time.Millisecond)
					}
					if (strings.HasPrefix(loss, "delay-") || strings.HasPrefix(loss, "late-")) && !released {
						t.Fatal("delayed packet never delivered")
					}
					if _, err := server.AcceptStream(false); !errors.Is(err, stream.ErrWouldBlock) {
						t.Fatal("unexpected extra stream", err)
					}
					wantCommit := loss != "all-server-response" && !strings.HasPrefix(loss, "late-")
					wantRemote := next
					if !wantCommit {
						wantRemote = old
						if client.multipath.paths[0].application.localMigration != nil || candidate.validation.state != PathValidationFailed {
							t.Fatal("failed validation retained candidate")
						}
					}
					if !dropped || candidate.committed != wantCommit || !finished || string(received) != "survives validation loss" || server.CurrentRemoteAddr() != wantRemote {
						t.Fatalf("loss=%v commit=%v finished=%v received=%q remote=%s", dropped, candidate.committed, finished, received, server.CurrentRemoteAddr())
					}
				})
			}
		}
	}
}
