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

func TestRuntimeMTUSearchAdmission(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	c.config.enablePathMTUDiscovery = true
	c.peerParams.MaxUDPPayloadSize = 1400
	a := c.multipath.paths[1].application
	if err := c.startRuntimeMTUSearches(); err != nil {
		t.Fatal(err)
	}
	if a.mtu != nil {
		t.Fatal("unbound path initialized")
	}
	if err := c.bindPreparedPath(1, netip.MustParseAddrPort("192.0.2.1:443"), netip.MustParseAddrPort("192.0.2.2:443")); err != nil {
		t.Fatal(err)
	}
	if err := c.startRuntimeMTUSearches(); err != nil {
		t.Fatal(err)
	}
	if a.mtu != nil {
		t.Fatal("unvalidated path initialized")
	}
	a.validation.state = PathValidated
	c.config.DisablePathMTUDiscovery = true
	if err := c.startRuntimeMTUSearches(); err != nil {
		t.Fatal(err)
	}
	if a.mtu != nil {
		t.Fatal("disabled discovery initialized")
	}
	c.config.DisablePathMTUDiscovery = false
	if err := c.startRuntimeMTUSearches(); err != nil {
		t.Fatal(err)
	}
	if a.mtu == nil || a.mtu.ceiling != 1400 || a.mtu.confirmed != 1200 {
		t.Fatal("peer cap/initial size")
	}
	original := a.mtu
	if err := c.startRuntimeMTUSearches(); err != nil || a.mtu != original {
		t.Fatal("reinitialized", err)
	}
}

func TestUDPAutomaticMTUDiscovery(t *testing.T) {
	if !udpSupportsMTUDiscovery {
		t.Skip("socket nonfragmentation unsupported")
	}
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, disabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/disabled=%t", v, disabled), func(t *testing.T) {
				client, template := testPair(t, v, streamParameters())
				client.config.EnableMigration = true
				template.config.EnableMigration = true
				client.config.DisablePathMTUDiscovery = disabled
				template.config.DisablePathMTUDiscovery = disabled
				open := func() *net.UDPConn {
					s, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { s.Close() })
					return s
				}
				ss, cs := open(), open()
				codec, _ := token.NewRetryTokens([32]byte{17}, nil, time.Minute)
				endpoint, err := NewServerEndpoint(template.config, codec, 4)
				if err != nil {
					t.Fatal(err)
				}
				sr, err := NewUDPServer(ss, endpoint)
				if err != nil {
					t.Fatal(err)
				}
				defer sr.Close()
				ce, err := NewClientEndpoint(client, ss.LocalAddr().(*net.UDPAddr).AddrPort())
				if err != nil {
					t.Fatal(err)
				}
				cr, err := NewUDPServer(cs, ce)
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
				for _, pair := range []struct {
					runtime *UDPServer
					conn    *Conn
				}{{cr, client}, {sr, server}} {
					if err := pair.runtime.Wait(ctx, func(*ServerEndpoint) (bool, error) {
						c := pair.conn
						if !c.confirmed || c.multipath == nil {
							return false, nil
						}
						a := c.multipath.paths[0].application
						if disabled {
							if a.mtu != nil {
								return false, fmt.Errorf("disabled discovery initialized")
							}
							return true, nil
						}
						if a.mtu == nil {
							return false, nil
						}
						if a.mtu.ceiling != 1452 {
							return false, fmt.Errorf("unexpected ceiling %d", a.mtu.ceiling)
						}
						return a.mtu.confirmed > 1200, nil
					}); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}
