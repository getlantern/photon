package interop

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

// A server authority can change implementations without losing availability
// when clients return opaque NEW_TOKEN values issued by the previous server.
func TestForeignNewTokenUpstream(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", v), func(t *testing.T) {
			ct, st := tlsConfigs(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			seedSocket, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer seedSocket.Close()
			seedTransport := &upstream.Transport{Conn: seedSocket}
			defer seedTransport.Close()
			seed, err := seedTransport.Listen(st, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}})
			if err != nil {
				t.Fatal(err)
			}
			defer seed.Close()
			address := seed.Addr().String()
			cache := &observedTokenStore{TokenStore: upstream.NewLRUTokenStore(4, 4), stored: make(chan struct{}, 4)}
			first, err := dialUpstreamAddr(t, ctx, address, ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}, TokenStore: cache})
			if err != nil {
				t.Fatal(err)
			}
			defer first.CloseWithError(0, "")
			firstPeer, err := seed.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer firstPeer.CloseWithError(0, "")
			select {
			case <-cache.stored:
			case <-ctx.Done():
				t.Fatal("upstream token not received", ctx.Err())
			}
			first.CloseWithError(0, "")
			if err := seed.Close(); err != nil {
				t.Fatal(err)
			}
			if err := seedTransport.Close(); err != nil {
				t.Fatal(err)
			}
			if err := seedSocket.Close(); err != nil {
				t.Fatal(err)
			}
			replacement, err := quic.ListenAddr(address, st, &quic.Config{Versions: []quic.Version{v}, EnableDatagrams: true})
			if err != nil {
				t.Fatal(err)
			}
			defer replacement.Close()
			socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer socket.Close()
			observer := &tokenPacketObserver{PacketConn: socket}
			tr := &upstream.Transport{Conn: observer}
			defer tr.Close()
			c, err := tr.Dial(ctx, replacement.Addr(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}, TokenStore: cache, EnableDatagrams: true})
			if err != nil {
				t.Fatal("foreign token prevented reconnect", err)
			}
			defer c.CloseWithError(0, "")
			peer, err := replacement.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer peer.CloseWithError(0, "")
			if !observer.firstHasToken.Load() || observer.retries.Load() == 0 {
				t.Fatal("foreign-token Retry fallback not exercised")
			}
			if err := c.SendDatagram([]byte("replacement server")); err != nil {
				t.Fatal(err)
			}
			b, err := peer.ReceiveDatagram(ctx)
			if err != nil || string(b) != "replacement server" {
				t.Fatal(string(b), err)
			}
			// The replacement also supplies usable evidence for a later connection.
			select {
			case <-cache.stored:
			case <-ctx.Done():
				t.Fatal("replacement token not received", ctx.Err())
			}
		})
	}
}
