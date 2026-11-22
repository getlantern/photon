package transport

import (
	"context"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestEndpointPreferredMigration(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, success := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/success=%v", version, success), func(t *testing.T) {
				client, server, address := preferredAddressPair(t, version)
				local := netip.MustParseAddrPort("127.0.0.1:4000")
				original := netip.MustParseAddrPort("127.0.0.1:443")
				preferred := netip.AddrPortFrom(netip.AddrFrom4(address.IPv4), address.IPv4Port)
				client.config.EnableMigration = true
				owner, err := newNetworkOwner(client, local, original)
				if err != nil {
					t.Fatal(err)
				}
				endpoint, err := NewClientEndpoint(client, original)
				if err != nil {
					t.Fatal(err)
				}
				endpoint.networks = map[*Conn]*networkOwner{client: owner}
				endpoint.locals = map[netip.AddrPort]bool{local: true}
				now := time.Unix(101, 0)
				out, err := endpoint.NextDatagram(now)
				if err != nil || out == nil || out.Peer != preferred || out.Local != local || len(out.Data) != 1200 {
					t.Fatal("automatic probe", out, err)
				}
				app := client.multipath.paths[0].application
				candidate := app.localMigration
				if candidate == nil || !client.preferredAttempted || app.network.remote != original {
					t.Fatal("admission moved established binding")
				}
				if success {
					response := makePeerPacket(t, server, applicationSpace, 1000, wire.PathResponseFrame{Data: candidate.validation.probes[0].token})
					got, _, err := endpoint.handleNetworkDatagram(context.Background(), response, local, preferred, now)
					if err != nil || got != client || candidate.validation.state != PathValidated {
						t.Fatal("candidate response routing", err)
					}
					if _, err := endpoint.NextDatagram(now); err != nil {
						t.Fatal(err)
					}
					if app.network.remote != preferred || !candidate.committed || app.localMigration != nil {
						t.Fatal("owner failed to commit validated destination")
					}
				} else {
					now = candidate.validation.expires
					if _, err := endpoint.NextDatagram(now); err != nil {
						t.Fatal(err)
					}
					if app.localMigration != nil || app.network.remote != original {
						t.Fatal("expired attempt failed to preserve original path")
					}
					for range 5 {
						if err := endpoint.startPreferredMigration(client, now); err != nil {
							t.Fatal(err)
						}
						if app.localMigration != nil {
							t.Fatal("failed preferred validation restarted")
						}
					}
				}
			})
		}
	}
}

func TestPreferredSocketSelection(t *testing.T) {
	for _, alternate := range []bool{false, true} {
		t.Run(fmt.Sprintf("alternate=%v", alternate), func(t *testing.T) {
			address := wire.PreferredAddress{IPv6: netip.MustParseAddr("2001:db8::3").As16(), IPv6Port: 8443, ConnectionID: []byte("prefer01")}
			client, _, _ := preferredAddressPair(t, wire.Version1, address)
			local := netip.MustParseAddrPort("192.0.2.1:4000")
			original := netip.MustParseAddrPort("192.0.2.2:443")
			if err := client.activateBasePath(local, original); err != nil {
				t.Fatal(err)
			}
			endpoint := &ServerEndpoint{locals: map[netip.AddrPort]bool{local: true}}
			chosen := netip.MustParseAddrPort("[2001:db8::1]:4000")
			if alternate {
				endpoint.locals[chosen] = true
				endpoint.locals[netip.MustParseAddrPort("[2001:db8::2]:4000")] = true
			}
			if err := endpoint.startPreferredMigration(client, time.Unix(101, 0)); err != nil {
				t.Fatal(err)
			}
			candidate := client.multipath.paths[0].application.localMigration
			if alternate {
				if candidate == nil || candidate.tuple.local != chosen || candidate.tuple.remote != netip.MustParseAddrPort("[2001:db8::3]:8443") {
					t.Fatal("wrong alternate socket", candidate)
				}
			} else if candidate != nil || client.preferredAttempted {
				t.Fatal("attempted without compatible socket")
			}
		})
	}
}
