package transport

import (
	"fmt"
	"github.com/getlantern/photon/internal/wire"
	"net/netip"
	"testing"
	"time"
)

func TestStagedProbeRealPeerExchange(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, base := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/base=%v", v, base), func(t *testing.T) {
				c, s := testPair(t, v, streamParameters())
				drivePair(t, c, s, false, false)
				old := netip.MustParseAddrPort("192.0.2.1:443")
				next := netip.MustParseAddrPort("192.0.2.1:445")
				remote := netip.MustParseAddrPort("192.0.2.2:444")
				if err := c.activateBasePath(old, remote); err != nil {
					t.Fatal(err)
				}
				if err := s.activateBasePath(remote, old); err != nil {
					t.Fatal(err)
				}
				c.multipath.baseOnly, s.multipath.baseOnly = base, base
				c.peerParams.DisableActiveMigration = false
				s.config.allowCandidateMigration, s.config.allowFreshCIDMigration = true, true
				now := time.Unix(101, 0)
				candidate, err := c.prepareLocalPathProbe(0, next, now)
				if err != nil {
					t.Fatal(err)
				}
				serverApp := s.multipath.paths[0].application
				for i := 0; i < 100; i++ {
					now = now.Add(time.Millisecond)
					raw, tuple, err := c.nextLocalMigrationProbe(0, now)
					if err != nil {
						t.Fatal(err)
					}
					if raw != nil {
						if err := s.handleBoundMultipathPacket(raw, remote, tuple.local, now); err != nil {
							t.Fatal(err)
						}
					}
					raw, tuple, err = s.nextMigrationProbe(0, now)
					if err != nil {
						t.Fatal(err)
					}
					if raw != nil {
						if err := c.handleBoundMultipathPacket(raw, tuple.remote, remote, now); err != nil {
							t.Fatal(err)
						}
					}
					if serverApp.network.remote != old {
						t.Fatal("server switched on probing-only packets")
					}
					if candidate.validation.state == PathValidated && serverApp.migration != nil && serverApp.migration.candidate != nil && serverApp.migration.candidate.validation.state == PathValidated {
						break
					}
				}
				if candidate.validation.state != PathValidated {
					t.Fatal("client did not receive real peer proof")
				}
				selection := serverApp.migration
				if selection == nil || selection.candidate == nil || selection.candidate.validation.state != PathValidated {
					t.Fatal("server mutual validation incomplete")
				}
				// New application data on the established tuple must retain probe proof.
				streamID, err := c.OpenStream(false)
				if err != nil {
					t.Fatal(err)
				}
				send := func(want netip.AddrPort) {
					t.Helper()
					if _, err := c.WriteStream(streamID, []byte("data"), false); err != nil {
						t.Fatal(err)
					}
					for range 100 {
						now = now.Add(time.Millisecond)
						_, tuple, raw, err := c.nextBoundMultipathPacket(now)
						if err != nil {
							t.Fatal(err)
						}
						if raw == nil {
							continue
						}
						decoded, err := s.authenticateMultipathPacket(raw, now)
						if err != nil || decoded == nil {
							t.Fatal(err)
						}
						hasStream := false
						for _, f := range decoded.packet.frames {
							if _, ok := f.(wire.StreamFrame); ok {
								hasStream = true
							}
						}
						if err := s.handleBoundMultipathPacket(raw, remote, tuple.local, now); err != nil {
							t.Fatal(err)
						}
						if hasStream {
							if tuple.local != want {
								t.Fatal("wrong client binding")
							}
							return
						}
					}
					t.Fatal("stream stalled")
				}
				send(old)
				if selection.candidate == nil || serverApp.network.remote != old {
					t.Fatal("old traffic discarded proof or migrated")
				}
				if err := c.commitLocalMigration(0, candidate); err != nil {
					t.Fatal(err)
				}
				send(next)
				if serverApp.network.remote != next {
					t.Fatal("server did not follow non-probing nomination")
				}
			})
		}
	}
}
