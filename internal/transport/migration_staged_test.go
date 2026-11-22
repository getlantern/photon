package transport

import (
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestStagedLocalProbePreservesBinding(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, base := range []bool{false, true} {
			for _, action := range []string{"switch", "cancel", "socket", "revoked"} {
				t.Run(fmt.Sprintf("%s/base=%v/%s", v, base, action), func(t *testing.T) {
					c, peer := testPair(t, v, streamParameters())
					drivePair(t, c, peer, false, false)
					old := netip.MustParseAddrPort("192.0.2.1:443")
					next := netip.MustParseAddrPort("192.0.2.1:445")
					remote := netip.MustParseAddrPort("192.0.2.2:444")
					if err := c.activateBasePath(old, remote); err != nil {
						t.Fatal(err)
					}
					if err := peer.activateBasePath(remote, old); err != nil {
						t.Fatal(err)
					}
					c.multipath.baseOnly, peer.multipath.baseOnly = base, base
					c.peerParams.DisableActiveMigration = false
					e, err := NewClientEndpoint(c, remote)
					if err != nil {
						t.Fatal(err)
					}
					e.locals = map[netip.AddrPort]bool{old: true, next: true}
					now := time.Unix(101, 0)
					candidate, err := c.prepareLocalPathProbe(0, next, now)
					if err != nil {
						t.Fatal(err)
					}
					raw, tuple, err := c.nextLocalMigrationProbe(0, now)
					if err != nil || raw == nil || tuple.local != next {
						t.Fatal(tuple, err)
					}
					decoded, err := peer.authenticateMultipathPacket(raw, now)
					if err != nil || decoded == nil {
						t.Fatal(err)
					}
					var challenge *wire.PathChallengeFrame
					for _, f := range decoded.packet.frames {
						switch f := f.(type) {
						case wire.PathChallengeFrame:
							copy := f
							challenge = &copy
						default:
							if f.Type() != 0 {
								t.Fatalf("probe nominated migration with frame %x", f.Type())
							}
						}
					}
					if challenge == nil {
						t.Fatal("no challenge")
					}
					app := c.multipath.paths[0].application
					// Ordinary output remains possible before and after reachability proof.
					streamID, err := c.OpenStream(false)
					if err != nil {
						t.Fatal(err)
					}
					checkOldOutput := func(label string) {
						t.Helper()
						if _, err := c.WriteStream(streamID, []byte(label), false); err != nil {
							t.Fatal(err)
						}
						for i := 0; i < 40; i++ {
							now = now.Add(time.Millisecond)
							_, where, pkt, err := c.nextBoundMultipathPacket(now)
							if err != nil {
								t.Fatal(err)
							}
							if pkt == nil {
								continue
							}
							decoded, err := peer.authenticateMultipathPacket(pkt, now)
							if err != nil || decoded == nil {
								t.Fatal(err)
							}
							for _, f := range decoded.packet.frames {
								if _, ok := f.(wire.StreamFrame); ok {
									if where.local != old {
										t.Fatal("application data moved before switch")
									}
									return
								}
							}
						}
						t.Fatal("probe blocked established-path data")
					}
					checkOldOutput("before proof")
					response, _ := (wire.PathResponseFrame{Data: challenge.Data}).Append(nil)
					proof := sealPathPayload(t, peer.writeKeys(applicationSpace), peer.peerCID, 0, peer.spaces[applicationSpace].nextPN+20, response)
					if err := c.handleBoundMultipathPacket(proof, next, remote, now); err != nil {
						t.Fatal(err)
					}
					if candidate.validation.state != PathValidated {
						t.Fatal("proof failed")
					}
					for range 3 {
						if err := e.serviceLocalMigrations(c); err != nil {
							t.Fatal(err)
						}
					}
					if candidate.committed || app.localMigration != candidate || app.network.local != old {
						t.Fatal("automatic switch")
					}
					checkOldOutput("after proof")
					if action != "switch" {
						switch action {
						case "cancel":
							err = c.cancelLocalMigration(0, candidate)
						case "socket":
							delete(e.locals, next)
							err = e.serviceLocalMigrations(c)
						case "revoked":
							delete(c.multipath.paths[0].peer.active, candidate.peerSequence)
							err = e.serviceLocalMigrations(c)
						}
						if err != nil {
							t.Fatal(err)
						}
						if candidate.committed || app.localMigration != nil || app.network.local != old {
							t.Fatal("cancellation switched or retained candidate")
						}
						if err := c.commitLocalMigration(0, candidate); err == nil {
							t.Fatal("stale candidate switched")
						}
						return
					}
					if err := c.commitLocalMigration(0, candidate); err != nil {
						t.Fatal(err)
					}
					if !candidate.committed || app.network.local != next || app.localMigration != nil {
						t.Fatal("explicit switch failed")
					}
				})
			}
		}
	}

}
