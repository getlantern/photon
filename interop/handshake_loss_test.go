package interop

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	independent "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlogwriter"
)

// Drop a selected datagram in one direction, covering a handshake flight or
// Retry response flight without depending on either implementation's coalescing.
func handshakeLossRelay(t *testing.T, server net.Addr, dropServer bool, ordinal int) (net.Addr, *atomic.Int64) {
	t.Helper()
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	trace := newHandshakeTrace(t)
	remote := server.(*net.UDPAddr)
	var dropped atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		buffer := make([]byte, 65535)
		var client *net.UDPAddr
		var towardServer, towardClient int
		for {
			n, from, err := socket.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			target := remote
			if from.AddrPort() == remote.AddrPort() {
				if client == nil {
					continue
				}
				target = client
				towardClient++
				if dropServer && towardClient == ordinal {
					dropped.Add(1)
					trace.packet("drop", from.AddrPort(), buffer[:n])
					continue
				}
			} else {
				client = from
				towardServer++
				if !dropServer && towardServer == ordinal {
					dropped.Add(1)
					trace.packet("drop", from.AddrPort(), buffer[:n])
					continue
				}
			}
			trace.packet("forward-from", from.AddrPort(), buffer[:n])
			if _, err := socket.WriteToUDP(buffer[:n], target); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { socket.Close(); <-done })
	return socket.LocalAddr(), &dropped
}

func TestPublicHandshakeSingleDatagramLoss(t *testing.T) {
	for _, version := range []independent.Version{independent.Version1, independent.Version2} {
		for _, independentServer := range []bool{false, true} {
			for _, dropServer := range []bool{false, true} {
				for _, ordinal := range []int{1, 2} {
					t.Run(fmt.Sprintf("%x/independent-server=%v/drop-server=%v/packet=%d", version, independentServer, dropServer, ordinal), func(t *testing.T) {
						clientTLS, serverTLS := tlsConfigs(t)
						peerTrace := newHandshakeTrace(t)
						ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
						defer cancel()
						var sender io.WriteCloser
						var accept func() (io.Reader, error)
						var dropped *atomic.Int64
						if independentServer {
							listener, err := independent.ListenAddr("127.0.0.1:0", serverTLS, nil)
							if err != nil {
								t.Fatal(err)
							}
							defer listener.Close()
							relay, count := handshakeLossRelay(t, listener.Addr(), dropServer, ordinal)
							dropped = count
							// Match the relay's address family. On Darwin a dual-stack
							// wildcard can overlap an existing IPv4 socket's port.
							peerSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero})
							if err != nil {
								t.Fatal(err)
							}
							defer peerSocket.Close()
							client, err := upstream.Dial(ctx, peerSocket, relay, clientTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, Tracer: func(context.Context, bool, upstream.ConnectionID) qlogwriter.Trace { return peerTrace }})
							if err != nil {
								t.Fatalf("dial after %d drops: %v", count.Load(), err)
							}
							defer client.CloseWithError(0, "")
							server, err := listener.Accept(ctx)
							if err != nil {
								t.Fatal(err)
							}
							sender, err = client.OpenUniStreamSync(ctx)
							if err != nil {
								t.Fatal(err)
							}
							accept = func() (io.Reader, error) {
								stream, err := server.AcceptUniStream(ctx)
								if err == nil {
									stream.SetReadDeadline(time.Now().Add(5 * time.Second))
								}
								return stream, err
							}
						} else {
							listener, err := upstream.ListenAddr("127.0.0.1:0", serverTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, Tracer: func(context.Context, bool, upstream.ConnectionID) qlogwriter.Trace { return peerTrace }})
							if err != nil {
								t.Fatal(err)
							}
							defer listener.Close()
							relay, count := handshakeLossRelay(t, listener.Addr(), dropServer, ordinal)
							dropped = count
							client, err := independent.DialAddr(ctx, relay.String(), clientTLS, &independent.Config{Versions: []independent.Version{version}, LocalAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}})
							if err != nil {
								t.Fatalf("dial after %d drops: %v", count.Load(), err)
							}
							local, remote, bound := client.PathAddresses(0)
							if !bound || local.String() != client.LocalAddr().String() || remote.String() != client.RemoteAddr().String() || client.ConnectionState().SupportsMultipath {
								t.Fatal("concrete client did not activate ordinary path ownership")
							}
							defer client.CloseWithError(0, "")
							server, err := listener.Accept(ctx)
							if err != nil {
								t.Fatal(err)
							}
							defer server.CloseWithError(0, "")
							sender, err = client.OpenUniStreamSync(ctx)
							if err != nil {
								t.Fatal(err)
							}
							accept = func() (io.Reader, error) {
								stream, err := server.AcceptUniStream(ctx)
								if err == nil {
									stream.SetReadDeadline(time.Now().Add(5 * time.Second))
								}
								return stream, err
							}
						}
						const payload = "stream delivery after handshake loss"
						if _, err := sender.Write([]byte(payload)); err != nil {
							t.Fatal(err)
						}
						if err := sender.Close(); err != nil {
							t.Fatal(err)
						}
						receiver, err := accept()
						if err != nil {
							t.Fatal(err)
						}
						data, err := io.ReadAll(receiver)
						if err != nil || string(data) != payload {
							t.Fatalf("stream: %q, %v", data, err)
						}
						if dropped.Load() != 1 {
							t.Fatalf("expected one deliberate drop, got %d", dropped.Load())
						}
					})
				}
			}
		}
	}
}
