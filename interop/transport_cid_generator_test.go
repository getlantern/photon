package interop

import (
	"context"
	"fmt"
	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/wire"
	upstream "github.com/quic-go/quic-go"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type cidLengthSocket struct {
	net.PacketConn
	want    int
	initial atomic.Int32
	bad     atomic.Bool
}

func (s *cidLengthSocket) WriteTo(b []byte, addr net.Addr) (int, error) {
	if h, err := wire.ParseHeader(b, 0); err == nil && h.Long && h.Type == wire.Initial {
		s.initial.Add(1)
		if len(h.SCID) != s.want {
			s.bad.Store(true)
		}
	}
	return s.PacketConn.WriteTo(b, addr)
}

func TestTransportConnectionIDGeneratorIndependent(t *testing.T) {
	custom := true
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for length := 0; length <= 20; length++ {
			t.Run(fmt.Sprintf("%v/%d", version, length), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				want := length
				if want == 0 && !custom {
					want = 4
				}
				sockets := make([]*cidLengthSocket, 1)
				transports := make([]*quic.Transport, 1)
				for i := range sockets {
					socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
					if err != nil {
						t.Fatal(err)
					}
					defer socket.Close()
					sockets[i] = &cidLengthSocket{PacketConn: socket, want: want}
					transports[i] = &quic.Transport{Conn: sockets[i], ConnectionIDLength: length}
					if custom {
						transports[i].ConnectionIDGenerator = &testCIDGenerator{length: length}
						transports[i].ConnectionIDLength = 99
					}
					defer transports[i].Close()
				}
				cfg := &quic.Config{Versions: []quic.Version{version}}
				l, err := transports[0].Listen(st, cfg)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				client, err := dialUpstreamAddr(t, ctx, l.Addr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}})
				if err != nil {
					t.Fatal(err)
				}
				defer client.CloseWithError(0, "")
				server, err := l.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				stream, err := client.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := stream.Write([]byte("CID length routing")); err != nil {
					t.Fatal(err)
				}
				stream.Close()
				received, err := server.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				received.SetReadDeadline(time.Now().Add(time.Second))
				b, err := io.ReadAll(received)
				if err != nil || string(b) != "CID length routing" {
					t.Fatal(string(b), err)
				}
				for _, s := range sockets {
					if s.bad.Load() || s.initial.Load() == 0 {
						t.Fatal("incorrect or absent initial SCID", want)
					}
				}
			})
		}
	}
}

// This counter fixture is intentionally predictable; production generators must
// supply unlinkable IDs. Calls are serialized by the Transport, not this fixture.
type testCIDGenerator struct {
	length int
	next   uint64
}

func (g *testCIDGenerator) ConnectionIDLen() int { return g.length }
func (g *testCIDGenerator) GenerateConnectionID() (quic.ConnectionID, error) {
	g.next++
	b := make([]byte, g.length)
	for i := range b {
		b[i] = byte(g.next >> (8 * (i % 8)))
	}
	return quic.ConnectionIDFromBytes(b), nil
}

func TestTransportDialCIDGeneratorIndependent(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for length := 0; length <= 20; length++ {
			t.Run(fmt.Sprintf("%v/%d", version, length), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				bind := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}
				serverSocket, err := net.ListenUDP("udp4", bind)
				if err != nil {
					t.Fatal(err)
				}
				defer serverSocket.Close()
				reference := &upstream.Transport{Conn: serverSocket}
				defer reference.Close()
				listener, err := reference.Listen(st, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}})
				if err != nil {
					t.Fatal(err)
				}
				socket, err := net.ListenUDP("udp4", bind)
				if err != nil {
					t.Fatal(err)
				}
				defer socket.Close()
				observed := &cidLengthSocket{PacketConn: socket, want: length}
				tr := &quic.Transport{Conn: observed, ConnectionIDGenerator: &testCIDGenerator{length: length}}
				defer tr.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				client, err := tr.Dial(ctx, listener.Addr(), ct, &quic.Config{Versions: []quic.Version{version}})
				if err != nil {
					t.Fatal(err)
				}
				server, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				stream, err := server.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := stream.Write([]byte("independent server routing")); err != nil {
					t.Fatal(err)
				}
				stream.Close()
				receive, err := client.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				receive.SetReadDeadline(time.Now().Add(time.Second))
				got, err := io.ReadAll(receive)
				if err != nil || string(got) != "independent server routing" {
					t.Fatal(string(got), err)
				}
				if observed.bad.Load() || observed.initial.Load() == 0 {
					t.Fatal("incorrect custom client SCID")
				}
			})
		}
	}
}
