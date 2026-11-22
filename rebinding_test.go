package quic

import (
	"context"
	"fmt"
	"github.com/getlantern/photon/internal/transport"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPublicClientAddressRebinding(t *testing.T) {
	for _, multipath := range []bool{false, true} {
		for _, version := range []Version{Version1, Version2} {
			for _, fresh := range []bool{false, true} {
				for _, ipChanged := range []bool{false, true} {
					t.Run(fmt.Sprintf("%x/fresh=%v/ip=%v/multipath=%v", version, fresh, ipChanged, multipath), func(t *testing.T) {
						clientTLS, serverTLS := dialTLS(t)
						listener, err := ListenAddr("127.0.0.1:0", serverTLS, &Config{EnableMultipath: multipath})
						if err != nil {
							t.Fatal(err)
						}
						defer listener.Close()
						open := func() *net.UDPConn {
							t.Helper()
							s, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
							if err != nil {
								t.Fatal(err)
							}
							t.Cleanup(func() { s.Close() })
							return s
						}
						front, oldBack := open(), open()
						var newBack *net.UDPConn
						if ipChanged {
							newBack = alternateLocalIPv4Socket(t)
						} else {
							newBack = open()
						}
						var active atomic.Pointer[net.UDPConn]
						var clientAddress atomic.Pointer[net.UDPAddr]
						active.Store(oldBack)
						var workers sync.WaitGroup
						workers.Add(3)
						go func() {
							defer workers.Done()
							buffer := make([]byte, 65535)
							for {
								n, from, err := front.ReadFromUDP(buffer)
								if err != nil {
									return
								}
								clientAddress.Store(from)
								if _, err = active.Load().WriteToUDP(buffer[:n], listener.Addr().(*net.UDPAddr)); err != nil {
									return
								}
							}
						}()
						for _, back := range []*net.UDPConn{oldBack, newBack} {
							go func() {
								defer workers.Done()
								buffer := make([]byte, 65535)
								for {
									n, _, err := back.ReadFromUDP(buffer)
									if err != nil {
										return
									}
									if addr := clientAddress.Load(); addr != nil {
										if _, err = front.WriteToUDP(buffer[:n], addr); err != nil {
											return
										}
									}
								}
							}()
						}
						defer func() { front.Close(); oldBack.Close(); newBack.Close(); workers.Wait() }()
						ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
						defer cancel()
						client, err := DialAddr(ctx, front.LocalAddr().String(), clientTLS, &Config{Versions: []Version{version}, EnableMultipath: multipath, LocalAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}})
						if err != nil {
							t.Fatal(err)
						}
						defer client.CloseWithError(0, "")
						server, err := listener.Accept(ctx)
						if err != nil {
							t.Fatal(err)
						}
						transfer := func(from, to *Conn, payload string) {
							t.Helper()
							send, err := from.OpenUniStreamSync(ctx)
							if err != nil {
								t.Fatal(err)
							}
							send.SetWriteDeadline(time.Now().Add(5 * time.Second))
							if _, err := send.Write([]byte(payload)); err != nil {
								t.Fatal(err)
							}
							if err := send.Close(); err != nil {
								t.Fatal(err)
							}
							receive, err := to.AcceptUniStream(ctx)
							if err != nil {
								t.Fatal(err)
							}
							receive.SetReadDeadline(time.Now().Add(5 * time.Second))
							got, err := io.ReadAll(receive)
							if err != nil {
								t.Fatal(err)
							}
							if string(got) != payload {
								t.Fatalf("received %q, want %q", got, payload)
							}
						}
						// Rebind before the first stream: handshake CID evidence must suffice.
						for i, back := range []*net.UDPConn{newBack, oldBack} {
							if fresh {
								if err := client.server.Wait(ctx, func(*transport.ServerEndpoint) (bool, error) { return client.raw.CanRotatePeerID(), client.raw.Err() }); err != nil {
									t.Fatal(err)
								}
								if err := client.server.Do(ctx, func(*transport.ServerEndpoint) error {
									if err := client.raw.RotatePeerID(); err != nil {
										return err
									}
									active.Store(back)
									return nil
								}); err != nil {
									t.Fatal(err)
								}
							} else {
								active.Store(back)
							}
							transfer(client, server, fmt.Sprintf("after rebinding %d", i))
							if got := server.RemoteAddr().String(); got != back.LocalAddr().String() {
								t.Fatalf("remote %s, want %s", got, back.LocalAddr())
							}

							_, remote, ok := server.PathAddresses(0)
							if !ok || remote.String() != back.LocalAddr().String() {
								t.Fatal("path zero snapshot did not follow rebinding")
							}
							transfer(server, client, "reply on validated port")
						}
						if err := listener.Close(); err != nil {
							t.Fatal(err)
						}
						if server.RemoteAddr().String() != oldBack.LocalAddr().String() {
							t.Fatal("shutdown lost committed address")
						}
					})
				}
			}
		}
	}
}

// Prefer another loopback address. macOS often has only 127.0.0.1 assigned,
// so fall back to another IPv4 address already owned by this host. All relay
// traffic still targets local sockets; this does not configure any interface.
func alternateLocalIPv4Socket(t *testing.T) *net.UDPConn {
	t.Helper()
	candidates := []net.IP{net.IPv4(127, 0, 0, 2)}
	addresses, err := net.InterfaceAddrs()
	if err == nil {
		for _, address := range addresses {
			ip, _, err := net.ParseCIDR(address.String())
			if err == nil && ip.To4() != nil && !ip.Equal(net.IPv4(127, 0, 0, 1)) && !ip.IsUnspecified() && !ip.IsMulticast() {
				candidates = append(candidates, ip)
			}
		}
	}
	for _, ip := range candidates {
		socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: ip})
		if err != nil {
			continue
		}
		t.Cleanup(func() { socket.Close() })
		return socket
	}
	t.Skip("peer-IP rebinding requires a second locally owned IPv4 address")
	return nil
}
