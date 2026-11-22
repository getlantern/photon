package transport

import (
	"container/list"
	"context"
	"fmt"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestPreferredCandidateSocketFailure(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			client, server, address := preferredAddressPair(t, version)
			local := netip.MustParseAddrPort("127.0.0.1:4000")
			original := netip.MustParseAddrPort("127.0.0.1:443")
			preferred := netip.AddrPortFrom(netip.AddrFrom4(address.IPv4), address.IPv4Port)
			if err := client.activateBasePath(local, original); err != nil {
				t.Fatal(err)
			}
			if err := server.activateBasePath(original, local); err != nil {
				t.Fatal(err)
			}
			server.config.allowCandidateMigration, server.config.allowFreshCIDMigration = true, true
			now := time.Unix(101, 0)
			if _, err := client.preparePreferredMigration(local, preferred, now); err != nil {
				t.Fatal(err)
			}
			raw, _, err := client.nextLocalMigrationProbe(0, now)
			if err != nil || raw == nil {
				t.Fatal("missing client probe", err)
			}
			if err := server.handleBoundMultipathPacket(raw, preferred, local, now); err != nil {
				t.Fatal(err)
			}
			app := server.multipath.paths[0].application
			if app.migration == nil || app.migration.candidate == nil {
				t.Fatal("missing server candidate")
			}
			// Use its reserved CID before losing the socket, requiring retirement.
			if raw, _, err := server.nextMigrationProbe(0, now); err != nil || raw == nil {
				t.Fatal("missing server response", err)
			}
			candidate := app.migration.candidate
			sequence := candidate.peerSequence
			router, err := NewRouter(len(server.config.LocalCID), 1)
			if err != nil {
				t.Fatal(err)
			}
			endpoint := &ServerEndpoint{router: router, readyNodes: make(map[*Conn]*list.Element), announced: make(map[*Conn]bool)}
			if err := endpoint.router.Add(server, local); err != nil {
				t.Fatal(err)
			}
			endpoint.conns = []*Conn{server}
			endpoint.networks = map[*Conn]*networkOwner{server: {conn: server, primary: pathTuple{original, local}, activationChecked: true}}
			endpoint.locals = map[netip.AddrPort]bool{original: true, preferred: true}
			endpoint.failLocalSocket(preferred, errSocketRemoved)
			if app.migration.candidate != nil {
				t.Fatal("failed preferred socket retained candidate")
			}
			if err := server.queueFrameOnPath(0, wire.PingFrame{}); err != nil {
				t.Fatal(err)
			}
			sent := false
			for i := 0; i < 10; i++ {
				output, err := endpoint.NextDatagram(now.Add(time.Duration(i) * 100 * time.Millisecond))
				if err != nil {
					t.Fatal(err)
				}
				if output != nil {
					sent = true
				}
				if output != nil && output.Local != original {
					t.Fatal("output used failed socket")
				}
			}
			if !sent {
				t.Fatal("original path produced no output")
			}
			if server.closed || len(endpoint.conns) != 1 || app.network.local != original {
				t.Fatal("original path lost")
			}
			if _, ok := server.peerIDs.active[sequence]; ok {
				t.Fatal("used candidate CID not retired")
			}
		})
	}
}

// Close the actual UDP socket, letting its reader report failure to the owner.
// TLS is completed by the authenticated fixture before runtime handoff.
func TestPreferredCandidateUDPReaderFailure(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, network := range []string{"udp4", "udp6"} {
			t.Run(fmt.Sprintf("%x/%s", version, network), func(t *testing.T) {
				ip := net.IPv4(127, 0, 0, 1)
				if network == "udp6" {
					ip = net.IPv6loopback
				}
				open := func() *net.UDPConn {
					t.Helper()
					socket, err := net.ListenUDP(network, &net.UDPAddr{IP: ip})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { socket.Close() })
					return socket
				}
				originalSocket, preferredSocket, clientSocket := open(), open(), open()
				original := originalSocket.LocalAddr().(*net.UDPAddr).AddrPort()
				preferred := preferredSocket.LocalAddr().(*net.UDPAddr).AddrPort()
				local := clientSocket.LocalAddr().(*net.UDPAddr).AddrPort()
				address := wire.PreferredAddress{ConnectionID: []byte("prefer01"), StatelessResetToken: [16]byte{41}}
				if network == "udp4" {
					address.IPv4, address.IPv4Port = preferred.Addr().As4(), preferred.Port()
				} else {
					address.IPv6, address.IPv6Port = preferred.Addr().As16(), preferred.Port()
				}
				client, server, _ := preferredAddressPair(t, version, address)
				server.config.EnableMigration = true
				server.config.allowCandidateMigration, server.config.allowFreshCIDMigration = true, true
				server.config.DisablePathMTUDiscovery = true
				if err := client.activateBasePath(local, original); err != nil {
					t.Fatal(err)
				}
				router, err := NewRouter(len(server.config.LocalCID), 1)
				if err != nil {
					t.Fatal(err)
				}
				if err := router.Add(server, local); err != nil {
					t.Fatal(err)
				}
				endpoint := &ServerEndpoint{config: server.config, router: router, conns: []*Conn{server}, readyNodes: make(map[*Conn]*list.Element), announced: make(map[*Conn]bool)}
				runtime, err := newMultiUDPServer([]*net.UDPConn{originalSocket, preferredSocket}, endpoint)
				if err != nil {
					t.Fatal(err)
				}
				defer runtime.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				now := time.Now()
				if _, err := client.preparePreferredMigration(local, preferred, now); err != nil {
					t.Fatal(err)
				}
				raw, _, err := client.nextLocalMigrationProbe(0, now)
				if err != nil || raw == nil {
					t.Fatal("missing probe", err)
				}
				if _, err := clientSocket.WriteToUDPAddrPort(raw, preferred); err != nil {
					t.Fatal(err)
				}
				if err := runtime.Wait(ctx, func(*ServerEndpoint) (bool, error) {
					if err := server.check(); err != nil {
						return false, err
					}
					if server.multipath == nil {
						return false, nil
					}
					app := server.multipath.paths[0].application
					return app.migration != nil && app.migration.candidate != nil && app.migration.candidate.hasPeerSequence, nil
				}); err != nil {
					t.Fatal("candidate admission", err)
				}
				if err := preferredSocket.Close(); err != nil {
					t.Fatal(err)
				}
				if err := runtime.Wait(ctx, func(e *ServerEndpoint) (bool, error) {
					if err := server.check(); err != nil {
						return false, err
					}
					app := server.multipath.paths[0].application
					return !e.locals[preferred] && app.migration.candidate == nil, nil
				}); err != nil {
					t.Fatal("reader failure cleanup", err)
				}
				ping := makePeerPacket(t, client, applicationSpace, 2000, wire.PingFrame{})
				if _, err := clientSocket.WriteToUDPAddrPort(ping, original); err != nil {
					t.Fatal(err)
				}
				if err := runtime.Wait(ctx, func(*ServerEndpoint) (bool, error) {
					if err := server.check(); err != nil {
						return false, err
					}
					app := server.multipath.paths[0].application
					if app.network.local != original {
						return false, fmt.Errorf("original binding changed")
					}
					return app.space.received.Expected() == 2001, nil
				}); err != nil {
					t.Fatal("original traffic after failure", err)
				}
				if err := runtime.Do(ctx, func(*ServerEndpoint) error { return server.queueFrameOnPath(0, wire.PingFrame{}) }); err != nil {
					t.Fatal(err)
				}
				deadline, _ := ctx.Deadline()
				clientSocket.SetReadDeadline(deadline)
				found := false
				for !found {
					var data [65535]byte
					n, from, err := clientSocket.ReadFromUDPAddrPort(data[:])
					if err != nil {
						t.Fatal("original-path reply", err)
					}
					if from != original {
						continue
					} // Buffered pre-failure probes.
					h, err := wire.ParseHeader(data[:n], len(client.config.LocalCID))
					if err != nil {
						t.Fatal(err)
					}
					_, plain, _, err := client.readKeys(applicationSpace).Open(data[:n], h.PNOffset, client.spaces[applicationSpace].received.Expected())
					if err != nil {
						t.Fatal(err)
					}
					frames, _, err := parsePacketFrames(plain, wire.FrameContext{PacketType: wire.OneRTT, SenderIsServer: true})
					if err != nil {
						t.Fatal(err)
					}
					for _, frame := range frames {
						if _, ok := frame.(wire.PingFrame); ok {
							found = true
						}
					}
				}
			})
		}
	}
}
