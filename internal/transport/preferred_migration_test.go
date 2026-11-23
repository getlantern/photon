package transport

import (
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestPreferredMigrationValidationAndCommit(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, ipv6 := range []bool{false, true} {
			for _, expire := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/ipv6=%v/expire=%v", version, ipv6, expire), func(t *testing.T) {
					local := netip.MustParseAddrPort("192.0.2.1:4000")
					original := netip.MustParseAddrPort("192.0.2.2:443")
					preferred := netip.MustParseAddrPort("192.0.2.3:8443")
					address := wire.PreferredAddress{ConnectionID: []byte("prefer01"), StatelessResetToken: [16]byte{19}}
					if ipv6 {
						local = netip.MustParseAddrPort("[2001:db8::1]:4000")
						original = netip.MustParseAddrPort("[2001:db8::2]:443")
						preferred = netip.MustParseAddrPort("[2001:db8::3]:8443")
						address.IPv6, address.IPv6Port = preferred.Addr().As16(), preferred.Port()
					} else {
						address.IPv4, address.IPv4Port = preferred.Addr().As4(), preferred.Port()
					}
					client, server, _ := preferredAddressPair(t, version, address)
					if err := client.activateBasePath(local, original); err != nil {
						t.Fatal(err)
					}
					app := client.multipath.paths[0].application
					now := time.Unix(101, 0)
					// A peer's disable_active_migration does not prohibit its own preferred address.
					client.peerParams.DisableActiveMigration = true
					candidate, err := client.preparePreferredMigration(local, preferred, now)
					if err != nil {
						t.Fatal(err)
					}
					old := *app.network
					if err := client.commitLocalMigration(0, candidate); err == nil {
						t.Fatal("unvalidated switch")
					}
					raw, tuple, err := client.nextLocalMigrationProbe(0, now)
					if err != nil || len(raw) != 1200 || tuple != (pathTuple{local, preferred}) || *app.network != old {
						t.Fatal("incorrect probe", tuple, len(raw), err)
					}
					if expire {
						if _, _, err := client.nextLocalMigrationProbe(0, candidate.validation.expires); err != nil {
							t.Fatal(err)
						}
						if app.localMigration != nil || *app.network != old || client.peerIDs.current != 0 {
							t.Fatal("failed validation changed established path")
						}
						return
					}
					token := candidate.validation.probes[0].token
					response := makePeerPacket(t, server, applicationSpace, 1000, wire.PathResponseFrame{Data: token})
					forged := append([]byte(nil), response...)
					forged[len(forged)-1] ^= 1
					if err := client.handleBoundMultipathPacket(forged, local, preferred, now); err != nil {
						t.Fatal(err)
					}
					wrong := netip.AddrPortFrom(preferred.Addr(), preferred.Port()+1)
					if err := client.handleBoundMultipathPacket(response, local, wrong, now); err != nil {
						t.Fatal(err)
					}
					if candidate.validation.state == PathValidated {
						t.Fatal("unauthorized proof validated address")
					}
					if err := client.handleBoundMultipathPacket(response, local, preferred, now); err != nil {
						t.Fatal(err)
					}
					if candidate.validation.state != PathValidated || *app.network != old {
						t.Fatal("proof not staged for owner commit")
					}
					candidate.tuple.remote = wrong
					if err := client.commitLocalMigration(0, candidate); err == nil || *app.network != old {
						t.Fatal("commit did not recheck destination authorization")
					}
					candidate.tuple.remote = preferred
					if err := client.commitLocalMigration(0, candidate); err != nil {
						t.Fatal(err)
					}
					if app.network.remote != preferred || app.network.local != local || !candidate.committed || client.peerIDs.current != candidate.peerSequence {
						t.Fatal("preferred destination not committed")
					}
				})
			}
		}
	}
}

func TestPreferredMigrationRejectsUnauthorizedDestination(t *testing.T) {
	for _, scenario := range []string{"unknown", "zero-port", "unspecified", "absent", "unconfirmed", "server", "wrong-family", "disabled-original"} {
		t.Run(scenario, func(t *testing.T) {
			client, _, address := preferredAddressPair(t, wire.Version1)
			local := netip.MustParseAddrPort("127.0.0.1:4000")
			original := netip.MustParseAddrPort("127.0.0.1:443")
			remote := netip.AddrPortFrom(netip.AddrFrom4(address.IPv4), address.IPv4Port)
			if err := client.activateBasePath(local, original); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "unknown":
				remote = netip.MustParseAddrPort("127.0.0.2:8443")
			case "zero-port":
				client.peerParams.PreferredAddress.IPv4Port = 0
				remote = netip.AddrPortFrom(remote.Addr(), 0)
			case "unspecified":
				client.peerParams.PreferredAddress.IPv4 = [4]byte{}
				remote = netip.MustParseAddrPort("0.0.0.0:8443")
			case "absent":
				client.peerParams.PreferredAddress = nil
			case "unconfirmed":
				client.confirmed = false
			case "server":
				client.config.Server = true
			case "disabled-original":
				client.peerParams.DisableActiveMigration = true
				client.peerParams.PreferredAddress.IPv4Port = original.Port()
				remote = original
				local = netip.MustParseAddrPort("127.0.0.1:4001")
			case "wrong-family":
				local = netip.MustParseAddrPort("[::1]:4000")
			}
			if candidate, err := client.preparePreferredMigration(local, remote, time.Unix(101, 0)); candidate != nil || err == nil {
				t.Fatal("unauthorized migration accepted")
			}
			if client.multipath.paths[0].application.localMigration != nil {
				t.Fatal("rejection retained candidate")
			}
			for _, id := range client.peerIDs.active {
				if id.reservation != nil {
					t.Fatal("rejection reserved CID")
				}
			}
		})
	}
}
