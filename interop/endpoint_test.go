package interop

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/token"
	engine "github.com/getlantern/photon/internal/transport"
	"github.com/getlantern/photon/internal/wire"
	upstream "github.com/quic-go/quic-go"
)

func TestSharedServerEndpoint(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprintf("%x", uint32(v)), func(t *testing.T) {
			trace := newHandshakeTrace(t)
			clientTLS, serverTLS := tlsConfigs(t)
			socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer socket.Close()
			var key [32]byte
			rand.Read(key[:])
			codec, _ := token.NewRetryTokens(key, nil, time.Minute)
			endpoint, err := engine.NewServerEndpoint(engine.Config{Version: v, TLS: serverTLS, Parameters: wire.DefaultTransportParameters()}, codec, 2)
			if err != nil {
				t.Fatal(err)
			}
			defer endpoint.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			results := make(chan peerResult, 2)
			var clients [2]peerResult
			var workers sync.WaitGroup
			t.Cleanup(func() {
				cancel()
				workers.Wait()
				for _, r := range clients {
					if r.conn != nil {
						r.conn.CloseWithError(0, "")
					}
				}
			})
			for i := 0; i < 2; i++ {
				// Explicit IPv4 avoids dual-stack/IPv4 port overlap on Darwin.
				peerSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { peerSocket.Close() })
				workers.Add(1)
				go func() {
					defer workers.Done()
					c, err := upstream.Dial(ctx, peerSocket, socket.LocalAddr(), clientTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}})
					clients[i] = peerResult{c, err}
					results <- clients[i]
				}()
			}
			peers := map[*engine.Conn]bool{}
			accepted := map[*engine.Conn]bool{}
			defer func() {
				if !t.Failed() {
					return
				}
				t.Logf("endpoint peers=%d accepted=%d deadline=%s", len(peers), len(accepted), endpoint.Deadline())
				for c := range peers {
					t.Logf("peer=%p complete=%t confirmed=%t recovery=%s", c, c.HandshakeComplete(), c.HandshakeConfirmed(), c.RecoveryDeadline())
				}
			}()
			buffer := make([]byte, 65535)
			for {
				now := time.Now()
				if err := endpoint.HandleTimeout(now); err != nil {
					t.Fatal(err)
				}
				for {
					out, err := endpoint.NextDatagram(now)
					if err != nil {
						t.Fatal(err)
					}
					if out == nil {
						break
					}
					trace.packet("send", out.Peer, out.Data)
					if _, err := socket.WriteToUDPAddrPort(out.Data, out.Peer); err != nil {
						endpoint.Remove(out.Conn)
						t.Fatal(err)
					}
				}
				for {
					c, peer, err := endpoint.Accept()
					if errors.Is(err, engine.ErrAcceptWouldBlock) {
						break
					}
					if err != nil || !peer.IsValid() || !peers[c] || accepted[c] {
						t.Fatal("accept", err)
					}
					accepted[c] = true
				}
				ready := len(accepted) == 2
				for c := range peers {
					ready = ready && c.HandshakeConfirmed()
				}
				if ready {
					break
				}
				deadline, _ := ctx.Deadline()
				if at := endpoint.Deadline(); !at.IsZero() && at.Before(deadline) {
					deadline = at
				}
				socket.SetReadDeadline(deadline)
				n, from, err := socket.ReadFromUDPAddrPort(buffer)
				if err != nil {
					if timeout, ok := err.(net.Error); ok && timeout.Timeout() && ctx.Err() == nil {
						continue
					}
					t.Fatal(err)
				}
				trace.packet("receive", from, buffer[:n])
				c, response, err := endpoint.HandleDatagram(ctx, buffer[:n], from, time.Now())
				if err != nil {
					t.Fatal(err)
				}
				if c != nil {
					peers[c] = true
				}
				if response != nil {
					trace.packet("admission-send", response.Peer, response.Data)
					if _, err := socket.WriteToUDPAddrPort(response.Data, response.Peer); err != nil {
						t.Fatal(err)
					}
				}
			}
			for i := 0; i < 2; i++ {
				select {
				case r := <-results:
					if r.err != nil {
						t.Fatal(r.err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
		})
	}
}
