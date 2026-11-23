package interop

import (
	"context"
	"fmt"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

func TestServerLiveTicketKeyRotation(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", v), func(t *testing.T) {
			clientTLS, serverTLS := tlsConfigs(t)
			cache := &sharedTicketCache{stored: make(chan struct{}, 1)}
			clientTLS.ClientSessionCache = cache
			var oldKey, newKey [32]byte
			oldKey[0] = 11
			newKey[0] = 22 // Test-only keys.
			serverTLS.SetSessionTicketKeys([][32]byte{oldKey})
			l, err := quic.ListenAddr("127.0.0.1:0", serverTLS, &quic.Config{Versions: []quic.Version{v}, EnableDatagrams: true})
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var clients []*upstream.Conn
			var servers []*quic.Conn
			for stage := 0; stage < 3; stage++ {
				if stage == 1 {
					serverTLS.SetSessionTicketKeys([][32]byte{newKey, oldKey})
				}
				if stage == 2 {
					serverTLS.SetSessionTicketKeys([][32]byte{newKey})
				}
				c, err := dialUpstreamAddr(t, ctx, l.Addr().String(), clientTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}, EnableDatagrams: true})
				if err != nil {
					t.Fatal(err)
				}
				defer c.CloseWithError(0, "")
				peer, err := l.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				clients = append(clients, c)
				servers = append(servers, peer)
				want := stage == 1
				if c.ConnectionState().TLS.DidResume != want || peer.ConnectionState().TLS.DidResume != want {
					t.Fatalf("stage %d resumed %t/%t, want %t", stage, c.ConnectionState().TLS.DidResume, peer.ConnectionState().TLS.DidResume, want)
				}
				if stage == 0 {
					select {
					case <-cache.stored:
					case <-ctx.Done():
						t.Fatal("missing first ticket", ctx.Err())
					}
					cache.freeze()
				}
			}
			// Ticket retirement affects new handshakes, not established traffic keys.
			for i, c := range clients {
				payload := fmt.Sprintf("still connected %d", i)
				if err := c.SendDatagram([]byte(payload)); err != nil {
					t.Fatal(err)
				}
				if b, err := servers[i].ReceiveDatagram(ctx); err != nil || string(b) != payload {
					t.Fatalf("%q: %v", b, err)
				}
			}
		})
	}
}
