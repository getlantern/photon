package transport

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// The handshake is completed with an authenticated fixture before transferring
// the client into the real UDP owner. The peer below answers actual wire probes;
// this test is not independent-peer or server-advertisement coverage.
func TestUDPPreferredMigration(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, network := range []string{"udp4", "udp6"} {
			for _, respond := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/%s/respond=%v", version, network, respond), func(t *testing.T) {
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
					original, preferred, local := open(), open(), open()
					preferredAddr := preferred.LocalAddr().(*net.UDPAddr).AddrPort()
					address := wire.PreferredAddress{ConnectionID: []byte("prefer01"), StatelessResetToken: [16]byte{37}}
					if network == "udp4" {
						address.IPv4, address.IPv4Port = preferredAddr.Addr().As4(), preferredAddr.Port()
					} else {
						address.IPv6, address.IPv6Port = preferredAddr.Addr().As16(), preferredAddr.Port()
					}
					client, peer, _ := preferredAddressPair(t, version, address)
					client.config.EnableMigration = true
					// Isolate address validation from optional MTU discovery traffic.
					client.config.DisablePathMTUDiscovery = true
					endpoint, err := NewClientEndpoint(client, original.LocalAddr().(*net.UDPAddr).AddrPort())
					if err != nil {
						t.Fatal(err)
					}
					runtime, err := NewUDPServer(local, endpoint)
					if err != nil {
						t.Fatal(err)
					}
					defer runtime.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
					defer cancel()
					readFrames := func(socket *net.UDPConn) []wire.Frame {
						t.Helper()
						socket.SetReadDeadline(time.Now().Add(2 * time.Second))
						var data [65535]byte
						n, from, err := socket.ReadFromUDPAddrPort(data[:])
						if err != nil {
							t.Fatal(err)
						}
						if from != local.LocalAddr().(*net.UDPAddr).AddrPort() {
							t.Fatal("wrong source", from)
						}
						h, err := wire.ParseHeader(data[:n], len(peer.config.LocalCID))
						if err != nil {
							t.Fatal(err)
						}
						_, plain, _, err := peer.readKeys(applicationSpace).Open(data[:n], h.PNOffset, peer.spaces[applicationSpace].received.Expected())
						if err != nil {
							t.Fatal(err)
						}
						frames, _, err := parsePacketFrames(plain, wire.FrameContext{PacketType: wire.OneRTT, SenderIsServer: false})
						if err != nil {
							t.Fatal(err)
						}
						return frames
					}
					var challenge *wire.PathChallengeFrame
					for _, frame := range readFrames(preferred) {
						if _, ok := frame.(wire.PingFrame); ok {
							t.Fatal("preferred probe contained non-probing PING")
						}
						if f, ok := frame.(wire.PathChallengeFrame); ok {
							challenge = &f
						}
					}
					if challenge == nil {
						t.Fatal("preferred socket did not receive challenge")
					}
					// Non-probing traffic must retain the original destination before proof.
					ping := func() {
						t.Helper()
						if err := runtime.Do(ctx, func(*ServerEndpoint) error { return client.enqueue(applicationSpace, wire.PingFrame{}, 1) }); err != nil {
							t.Fatal(err)
						}
					}
					awaitPing := func(socket *net.UDPConn) {
						t.Helper()
						for i := 0; i < 16; i++ {
							for _, frame := range readFrames(socket) {
								if _, ok := frame.(wire.PingFrame); ok {
									return
								}
							}
						}
						t.Fatal("no PING received")
					}
					ping()
					awaitPing(original)
					if respond {
						response := makePeerPacket(t, peer, applicationSpace, 1000, wire.PathResponseFrame{Data: challenge.Data})
						if _, err := preferred.WriteToUDPAddrPort(response, local.LocalAddr().(*net.UDPAddr).AddrPort()); err != nil {
							t.Fatal(err)
						}
					}
					want := original.LocalAddr().(*net.UDPAddr).AddrPort()
					if respond {
						want = preferredAddr
					}
					if err := runtime.Wait(ctx, func(*ServerEndpoint) (bool, error) {
						if err := client.check(); err != nil {
							return false, err
						}
						app := client.multipath.paths[0].application
						return client.preferredAttempted && app.localMigration == nil && app.network.remote == want, nil
					}); err != nil {
						t.Fatal("migration outcome", err)
					}
					ping()
					if respond {
						awaitPing(preferred)
					} else {
						awaitPing(original)
					}
				})
			}
		}
	}
}
