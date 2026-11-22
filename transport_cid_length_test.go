package quic

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
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

func TestTransportConnectionIDLength(t *testing.T)    { testTransportConnectionIDLength(t, false) }
func TestTransportConnectionIDGenerator(t *testing.T) { testTransportConnectionIDLength(t, true) }
func testTransportConnectionIDLength(t *testing.T, custom bool) {
	for _, version := range []Version{Version1, Version2} {
		for length := 0; length <= 20; length++ {
			t.Run(fmt.Sprintf("%v/%d", version, length), func(t *testing.T) {
				ct, st := dialTLS(t)
				want := length
				if want == 0 && !custom {
					want = 4
				}
				sockets := make([]*cidLengthSocket, 2)
				transports := make([]*Transport, 2)
				for i := range sockets {
					socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
					if err != nil {
						t.Fatal(err)
					}
					defer socket.Close()
					sockets[i] = &cidLengthSocket{PacketConn: socket, want: want}
					transports[i] = &Transport{Conn: sockets[i], ConnectionIDLength: length}
					if custom {
						transports[i].ConnectionIDGenerator = &testCIDGenerator{length: length}
						transports[i].ConnectionIDLength = 99
					}
					defer transports[i].Close()
				}
				cfg := &Config{Versions: []Version{version}}
				l, err := transports[0].Listen(st, cfg)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				client, err := transports[1].Dial(ctx, l.Addr(), ct, cfg)
				if err != nil {
					t.Fatal(err)
				}
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
	if custom {
		return
	}
	for _, length := range []int{-1, 21} {
		tr := &Transport{ConnectionIDLength: length}
		if _, err := tr.init(1); err == nil || tr.runtime != nil {
			t.Fatal("invalid CID length accepted")
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
func (g *testCIDGenerator) GenerateConnectionID() (ConnectionID, error) {
	g.next++
	b := make([]byte, g.length)
	for i := range b {
		b[i] = byte(g.next >> (8 * (i % 8)))
	}
	return ConnectionIDFromBytes(b), nil
}
