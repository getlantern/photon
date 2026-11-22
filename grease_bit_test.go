package quic

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// packetTap reports every datagram crossing a Transport socket.
type packetTap struct {
	net.PacketConn
	in, out func([]byte, net.Addr)
}

func (c *packetTap) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, err := c.PacketConn.ReadFrom(b)
	if err == nil {
		c.in(b[:n], addr)
	}
	return n, addr, err
}
func (c *packetTap) WriteTo(b []byte, addr net.Addr) (int, error) {
	c.out(b, addr)
	return c.PacketConn.WriteTo(b, addr)
}

func TestPublicQUICBitGreasing(t *testing.T) {
	for _, v := range []Version{Version1, Version2} {
		for _, clientSupport := range []bool{false, true} {
			for _, serverSupport := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/client=%v/server=%v", v, clientSupport, serverSupport), func(t *testing.T) {
					ct, st := dialTLS(t)
					socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
					if err != nil {
						t.Fatal(err)
					}
					defer socket.Close()
					// The server socket sees both directions: the client may clear the
					// bit only if the server advertised support, and the reverse.
					var fromClient, fromServer atomic.Int64
					count := func(cleared *atomic.Int64) func([]byte, net.Addr) {
						return func(b []byte, _ net.Addr) {
							if len(b) > 0 && b[0]&0x40 == 0 {
								cleared.Add(1)
							}
						}
					}
					tr := &Transport{Conn: &packetTap{PacketConn: socket, in: count(&fromClient), out: count(&fromServer)}, VerifySourceAddress: func(net.Addr) bool { return true }}
					defer tr.Close()
					listener, err := tr.Listen(st, &Config{Versions: []Version{v}, EnableQUICBitGreasing: serverSupport})
					if err != nil {
						t.Fatal(err)
					}
					defer listener.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					client, err := DialAddr(ctx, listener.Addr().String(), ct, &Config{Versions: []Version{v}, EnableQUICBitGreasing: clientSupport})
					if err != nil {
						t.Fatal(err)
					}
					defer client.CloseWithError(0, "")
					server, err := listener.Accept(ctx)
					if err != nil {
						t.Fatal(err)
					}
					stream, err := client.OpenStreamSync(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := stream.Write([]byte{0}); err != nil {
						t.Fatal(err)
					}
					incoming, err := server.AcceptStream(ctx)
					if err != nil {
						t.Fatal(err)
					}
					// Each datagram draws the bit independently. Every round trip needs
					// one datagram per direction, so an all-set run is a 2^-64 event.
					var turn [1]byte
					for i := 0; i < 64; i++ {
						if _, err := io.ReadFull(incoming, turn[:]); err != nil {
							t.Fatal(err)
						}
						if _, err := incoming.Write(turn[:]); err != nil {
							t.Fatal(err)
						}
						if _, err := io.ReadFull(stream, turn[:]); err != nil {
							t.Fatal(err)
						}
						if _, err := stream.Write(turn[:]); err != nil {
							t.Fatal(err)
						}
					}
					if _, err := io.ReadFull(incoming, turn[:]); err != nil {
						t.Fatal(err)
					}
					if _, err := stream.Write([]byte("greased request")); err != nil {
						t.Fatal(err)
					}
					if err := stream.Close(); err != nil {
						t.Fatal(err)
					}
					payload, err := io.ReadAll(incoming)
					if err != nil || string(payload) != "greased request" {
						t.Fatal(string(payload), err)
					}
					if _, err := incoming.Write([]byte("greased response")); err != nil {
						t.Fatal(err)
					}
					if err := incoming.Close(); err != nil {
						t.Fatal(err)
					}
					payload, err = io.ReadAll(stream)
					if err != nil || string(payload) != "greased response" {
						t.Fatal(string(payload), err)
					}
					if cleared := fromClient.Load(); (cleared > 0) != serverSupport {
						t.Fatalf("client cleared the QUIC bit in %d datagrams, server support %v", cleared, serverSupport)
					}
					if cleared := fromServer.Load(); (cleared > 0) != clientSupport {
						t.Fatalf("server cleared the QUIC bit in %d datagrams, client support %v", cleared, clientSupport)
					}
				})
			}
		}
	}
}
