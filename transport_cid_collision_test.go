package quic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/transport"
)

type collidingCIDGenerator struct {
	testCIDGenerator
	first   atomic.Value
	collide atomic.Bool
}

func (g *collidingCIDGenerator) GenerateConnectionID() (ConnectionID, error) {
	if g.collide.Load() {
		return g.first.Load().(ConnectionID), nil
	}
	id, err := g.testCIDGenerator.GenerateConnectionID()
	g.first.CompareAndSwap(nil, id)
	return id, err
}

func TestTransportCIDCollisionPreservesExistingConnection(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		for _, length := range []int{0, 8} {
			t.Run(fmt.Sprintf("%v/%d", version, length), func(t *testing.T) {
				ct, st := dialTLS(t)
				listener, err := ListenAddr("127.0.0.1:0", st, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				defer socket.Close()
				generator := &collidingCIDGenerator{testCIDGenerator: testCIDGenerator{length: length}}
				tr := &Transport{Conn: socket, ConnectionIDGenerator: generator}
				defer tr.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				config := &Config{Versions: []Version{version}}
				client, err := tr.Dial(ctx, listener.Addr(), ct, config)
				if err != nil {
					t.Fatal(err)
				}
				server, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				generator.collide.Store(true)
				if c, err := tr.Dial(ctx, listener.Addr(), ct, config); c != nil || !errors.Is(err, transport.ErrRouteCollision) {
					t.Fatal("collision not rejected", err)
				}
				generator.collide.Store(false)
				stream, err := client.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := stream.Write([]byte("original route survives")); err != nil {
					t.Fatal(err)
				}
				stream.Close()
				receive, err := server.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				receive.SetReadDeadline(time.Now().Add(time.Second))
				got, err := io.ReadAll(receive)
				if err != nil || string(got) != "original route survives" {
					t.Fatal(string(got), err)
				}
				if length > 0 {
					// The failed registration must release capacity and preserve generator use.
					next, err := tr.Dial(ctx, listener.Addr(), ct, config)
					if err != nil {
						t.Fatal("dial after collision", err)
					}
					defer next.CloseWithError(0, "")
					if _, err := listener.Accept(ctx); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}
