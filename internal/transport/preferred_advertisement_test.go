package transport

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/wire"
)

func TestPreferredAdvertisementUDPHandshake(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, network := range []string{"udp4", "udp6"} {
			for _, removePreferred := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/%s/remove=%v", version, network, removePreferred), func(t *testing.T) {
					open := func() *net.UDPConn {
						t.Helper()
						ip := net.IPv4(127, 0, 0, 1)
						if network == "udp6" {
							ip = net.IPv6loopback
						}
						s, err := net.ListenUDP(network, &net.UDPAddr{IP: ip})
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { s.Close() })
						return s
					}
					original, preferred, local := open(), open(), open()
					preferredAddr := preferred.LocalAddr().(*net.UDPAddr).AddrPort()
					client, template := testPair(t, version, streamParameters())
					client.config.EnableMigration = true
					config := template.config
					config.EnableMigration = true
					config.PreferredAddresses = []netip.AddrPort{preferredAddr}
					codec, _ := token.NewRetryTokens([32]byte{61}, nil, time.Minute)
					endpoint, err := NewServerEndpoint(config, codec, 4)
					if err != nil {
						t.Fatal(err)
					}
					sr, err := newMultiUDPServer([]*net.UDPConn{original, preferred}, endpoint)
					if err != nil {
						t.Fatal(err)
					}
					defer sr.Close()
					want := preferredAddr
					if removePreferred {
						if err := sr.removeSocket(context.Background(), preferredAddr); err != nil {
							t.Fatal(err)
						}
						want = original.LocalAddr().(*net.UDPAddr).AddrPort()
					}
					ce, err := NewClientEndpoint(client, original.LocalAddr().(*net.UDPAddr).AddrPort())
					if err != nil {
						t.Fatal(err)
					}
					cr, err := NewUDPServer(local, ce)
					if err != nil {
						t.Fatal(err)
					}
					defer cr.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					server, _, err := sr.Accept(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if err := cr.Wait(ctx, func(*ServerEndpoint) (bool, error) {
						if err := client.check(); err != nil {
							return false, err
						}
						if client.multipath == nil {
							return false, nil
						}
						if removePreferred && client.peerParams.PreferredAddress != nil {
							return false, fmt.Errorf("advertised removed socket")
						}
						app := client.multipath.paths[0].application
						return app.network != nil && app.network.remote == want, nil
					}); err != nil {
						t.Fatal("automatic client migration", err)
					}
					if err := cr.Do(ctx, func(*ServerEndpoint) error { return client.enqueue(applicationSpace, wire.PingFrame{}, 1) }); err != nil {
						t.Fatal(err)
					}
					if err := sr.Wait(ctx, func(*ServerEndpoint) (bool, error) {
						if err := server.check(); err != nil {
							return false, err
						}
						if server.multipath == nil {
							return false, nil
						}
						app := server.multipath.paths[0].application
						return app.network != nil && app.network.local == want, nil
					}); err != nil {
						t.Fatal("server preferred commit", err)
					}
				})
			}
		}

	}

}

func TestPreferredAdvertisementRequiresSockets(t *testing.T) {
	_, template := testPair(t, wire.Version1)
	config := template.config
	config.EnableMigration = true
	config.PreferredAddresses = []netip.AddrPort{netip.MustParseAddrPort("127.0.0.1:8443")}
	if c, err := NewConn(config); err == nil {
		c.Close()
		t.Fatal("unregistered advertisement admitted")
	}
	codec, _ := token.NewRetryTokens([32]byte{62}, nil, time.Minute)
	endpoint, err := NewServerEndpoint(config, codec, 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := endpoint.bindNetworkOwner(netip.MustParseAddrPort("127.0.0.1:443")); err == nil || endpoint.config.preferredSocketsReady {
		t.Fatal("missing socket authorized advertisement")
	}
}

func TestPreferredAdvertisementPerConnectionIDs(t *testing.T) {
	_, template := testPair(t, wire.Version1)
	config := template.config
	config.EnableMigration = true
	config.PreferredAddresses = []netip.AddrPort{netip.MustParseAddrPort("127.0.0.1:8443")}
	config.preferredSocketsReady = true // Models the verified socket owner's admission gate.
	first, err := NewConn(config)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := NewConn(config)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	a, b := first.config.Parameters.PreferredAddress, second.config.Parameters.PreferredAddress
	if string(a.ConnectionID) == string(b.ConnectionID) || a.StatelessResetToken == b.StatelessResetToken {
		t.Fatal("preferred identity shared across connections")
	}
	if config.Parameters.PreferredAddress != nil {
		t.Fatal("shared template mutated")
	}
	for _, c := range []*Conn{first, second} {
		p := c.config.Parameters.PreferredAddress
		if string(p.ConnectionID) == string(c.config.LocalCID) || !c.localIDs.accept(p.ConnectionID) || c.localIDs.highest != 1 {
			t.Fatal("preferred identity not owned before TLS output")
		}
	}
}
