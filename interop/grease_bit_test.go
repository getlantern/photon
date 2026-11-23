package interop

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	own "github.com/getlantern/photon"
	peer "github.com/quic-go/quic-go"
)

// This transparent relay counts only short-header first bytes. It neither
// decrypts nor alters traffic. Every forwarded UDP datagram is preserved intact.
type bitCounts struct{ set, clear, dropped atomic.Uint64 }
type bitRelay struct {
	address        string
	client, server bitCounts
}

func newBitRelay(t *testing.T, target string, dropEvery ...uint64) *bitRelay {
	t.Helper()
	remote, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		t.Fatal(err)
	}
	localIP := net.IPv4(127, 0, 0, 1)
	if remote.IP.To4() == nil {
		localIP = net.IPv6loopback
	}
	front, err := net.ListenUDP("udp", &net.UDPAddr{IP: localIP})
	if err != nil {
		t.Fatal(err)
	}
	back, err := net.DialUDP("udp", nil, remote)
	if err != nil {
		front.Close()
		t.Fatal(err)
	}
	relay := &bitRelay{address: front.LocalAddr().String()}
	var client atomic.Pointer[net.UDPAddr]
	var workers sync.WaitGroup
	workers.Add(2)
	period := uint64(0)
	if len(dropEvery) > 0 {
		period = dropEvery[0]
	}
	record := func(b []byte, c *bitCounts) bool {
		if len(b) > 0 && b[0]&0x80 == 0 {
			if b[0]&0x40 == 0 {
				c.clear.Add(1)
			} else {
				c.set.Add(1)
			}
			total := c.set.Load() + c.clear.Load()
			if period != 0 && total%period == 0 {
				c.dropped.Add(1)
				return false
			}
		}
		return true
	}
	go func() {
		defer workers.Done()
		buffer := make([]byte, 65535)
		for {
			n, addr, err := front.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			client.Store(addr)
			if !record(buffer[:n], &relay.client) {
				continue
			}
			if _, err := back.Write(buffer[:n]); err != nil {
				return
			}
		}
	}()
	go func() {
		defer workers.Done()
		buffer := make([]byte, 65535)
		for {
			n, err := back.Read(buffer)
			if err != nil {
				return
			}
			addr := client.Load()
			if addr == nil {
				continue
			}
			if !record(buffer[:n], &relay.server) {
				continue
			}
			if _, err := front.WriteToUDP(buffer[:n], addr); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { front.Close(); back.Close(); workers.Wait() })
	return relay
}

func checkBitCounts(t *testing.T, name string, counts *bitCounts, permission bool) {
	t.Helper()
	set, clear := counts.set.Load(), counts.clear.Load()
	if set+clear < 48 {
		t.Fatalf("%s: insufficient wire observations: set=%d clear=%d", name, set, clear)
	}
	if permission {
		if set == 0 || clear == 0 {
			t.Fatalf("%s: negotiated bit never varied: set=%d clear=%d", name, set, clear)
		}
	} else if clear != 0 {
		t.Fatalf("%s: cleared without receiver permission: set=%d clear=%d", name, set, clear)
	}
	t.Logf("%s set=%d clear=%d", name, set, clear)
}

func TestQUICBitWireObservations(t *testing.T) {
	for _, host := range []string{"127.0.0.1", "::1"} {
		for _, loss := range []uint64{0, 17} {
			for _, version := range []own.Version{own.Version1, own.Version2} {
				for _, clientSupport := range []bool{false, true} {
					for _, serverSupport := range []bool{false, true} {
						t.Run(fmt.Sprintf("%x/%s/loss=%d/client=%v/server=%v", version, host, loss, clientSupport, serverSupport), func(t *testing.T) {
							ct, st := tlsConfigs(t)
							listener, err := own.ListenAddr(net.JoinHostPort(host, "0"), st, &own.Config{Versions: []own.Version{version}, EnableQUICBitGreasing: serverSupport})
							if err != nil {
								t.Fatal(err)
							}
							defer listener.Close()
							relay := newBitRelay(t, listener.Addr().String(), loss)
							ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
							defer cancel()
							client, err := own.DialAddr(ctx, relay.address, ct, &own.Config{Versions: []own.Version{version}, EnableQUICBitGreasing: clientSupport})
							if err != nil {
								t.Fatal(err)
							}
							defer client.CloseWithError(0, "")
							server, err := listener.Accept(ctx)
							if err != nil {
								t.Fatal(err)
							}
							outgoing, err := client.OpenStreamSync(ctx)
							if err != nil {
								t.Fatal(err)
							}
							done := make(chan error, 1)
							workerDone := make(chan struct{})
							t.Cleanup(func() { <-workerDone })
							go func() {
								defer close(workerDone)
								incoming, err := server.AcceptStream(ctx)
								if err != nil {
									done <- err
									return
								}
								var data [1]byte
								for i := 0; i < 48; i++ {
									if _, err := io.ReadFull(incoming, data[:]); err != nil {
										done <- err
										return
									}
									if _, err := incoming.Write(data[:]); err != nil {
										done <- err
										return
									}
								}
								done <- incoming.Close()
							}()
							var value [1]byte
							for i := 0; i < 48; i++ {
								value[0] = byte(i)
								if _, err := outgoing.Write(value[:]); err != nil {
									t.Fatal(err)
								}
								if _, err := io.ReadFull(outgoing, value[:]); err != nil || value[0] != byte(i) {
									t.Fatal("echo", i, err)
								}
							}
							if err := outgoing.Close(); err != nil {
								t.Fatal(err)
							}
							if err := <-done; err != nil {
								t.Fatal(err)
							}
							checkBitCounts(t, "client sends", &relay.client, serverSupport)
							checkBitCounts(t, "server sends", &relay.server, clientSupport)
							if loss != 0 && (relay.client.dropped.Load() == 0 || relay.server.dropped.Load() == 0) {
								t.Fatal("loss scenario did not drop in both directions")
							}
							t.Logf("dropped client=%d server=%d", relay.client.dropped.Load(), relay.server.dropped.Load())
						})
					}
				}
			}
		}
	}
}

// The pinned upstream does not advertise grease_quic_bit. Our advertisement
// must not be mistaken for peer send permission in either version.
func TestQUICBitUpstreamFallback(t *testing.T) {
	for _, host := range []string{"127.0.0.1", "::1"} {
		for _, version := range []own.Version{own.Version1, own.Version2} {
			t.Run(fmt.Sprintf("%x/%s", version, host), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				listener, err := peer.ListenAddr(net.JoinHostPort(host, "0"), st, &peer.Config{Versions: []peer.Version{peer.Version(version)}})
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				relay := newBitRelay(t, listener.Addr().String())
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				client, err := own.DialAddr(ctx, relay.address, ct, &own.Config{Versions: []own.Version{version}, EnableQUICBitGreasing: true})
				if err != nil {
					t.Fatal(err)
				}
				defer client.CloseWithError(0, "")
				server, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer server.CloseWithError(0, "")
				stream, err := client.OpenStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				workerDone := make(chan struct{})
				t.Cleanup(func() { <-workerDone })
				go func() {
					defer close(workerDone)
					incoming, err := server.AcceptStream(ctx)
					if err != nil {
						done <- err
						return
					}
					_, err = io.Copy(incoming, incoming)
					if err == nil {
						err = incoming.Close()
					}
					done <- err
				}()
				// Keep the v2 exchange below the pinned peer's separate key-update gate.
				value := []byte("fallback")
				for i := 0; i < 12; i++ {
					if _, err := stream.Write(value); err != nil {
						t.Fatal(err)
					}
					got := make([]byte, len(value))
					if _, err := io.ReadFull(stream, got); err != nil || string(got) != string(value) {
						t.Fatal(err)
					}
				}
				stream.Close()
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				if relay.client.clear.Load() != 0 || relay.server.clear.Load() != 0 || relay.client.set.Load() < 12 || relay.server.set.Load() < 12 {
					t.Fatal("unnegotiated greasing or missing wire evidence")
				}
			})
		}
	}
}
