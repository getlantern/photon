package interop

import (
	"context"
	"crypto/rand"
	"fmt"
	"github.com/getlantern/photon/internal/token"
	"net"
	"sync/atomic"
	"testing"
	"time"

	engine "github.com/getlantern/photon/internal/transport"
	"github.com/getlantern/photon/internal/wire"
	upstream "github.com/quic-go/quic-go"
)

func TestUpstreamRequiresRetry(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprintf("%x", uint32(v)), func(t *testing.T) {
			clientTLS, serverTLS := tlsConfigs(t)
			upSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer upSocket.Close()
			var retries atomic.Int32
			tr := &upstream.Transport{Conn: upSocket, VerifySourceAddress: func(net.Addr) bool { retries.Add(1); return true }}
			defer tr.Close()
			listener, err := tr.Listen(serverTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer socket.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			accepted := make(chan peerResult, 1)
			go func() { c, err := listener.Accept(ctx); accepted <- peerResult{c, err} }()
			c, err := engine.NewConn(engine.Config{Version: v, TLS: clientTLS, LocalCID: []byte("client00"), PeerCID: []byte("original"), OriginalDCID: []byte("original"), Parameters: wire.DefaultTransportParameters()})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if err := c.Start(ctx); err != nil {
				t.Fatal(err)
			}
			if err := drive(ctx, c, socket, upSocket.LocalAddr().(*net.UDPAddr)); err != nil {
				t.Fatal(err)
			}
			select {
			case r := <-accepted:
				if r.err != nil {
					t.Fatal(r.err)
				}
				defer r.conn.CloseWithError(0, "")
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if retries.Load() == 0 || !c.HandshakeConfirmed() {
				t.Fatal("Retry handshake not exercised")
			}
		})
	}
}

func TestIndependentServerRequiresRetry(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprintf("%x", uint32(v)), func(t *testing.T) {
			clientTLS, serverTLS := tlsConfigs(t)
			socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer socket.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result := make(chan peerResult, 1)
			go func() {
				c, err := dialUpstreamAddr(t, ctx, socket.LocalAddr().String(), clientTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}})
				result <- peerResult{c, err}
			}()
			var key [32]byte
			if _, err := rand.Read(key[:]); err != nil {
				t.Fatal(err)
			}
			codec, _ := token.NewRetryTokens(key, nil, time.Minute)
			buffer := make([]byte, 65535)
			deadline, _ := ctx.Deadline()
			socket.SetReadDeadline(deadline)
			retries := 0
			var server *engine.Conn
			var peer *net.UDPAddr
			for server == nil {
				n, from, err := socket.ReadFromUDP(buffer)
				if err != nil {
					t.Fatal(err)
				}
				c, response, err := engine.AdmitInitial(ctx, engine.Config{Version: v, TLS: serverTLS, Parameters: wire.DefaultTransportParameters()}, codec, buffer[:n], from.AddrPort(), time.Now())
				if err != nil {
					if c != nil {
						c.Close()
					}
					t.Fatal(err)
				}
				if len(response) > 0 {
					retries++
					if _, err := socket.WriteToUDP(response, from); err != nil {
						t.Fatal(err)
					}
				}
				if c != nil {
					server = c
					peer = from
				}
			}
			defer server.Close()
			if err := drive(ctx, server, socket, peer); err != nil {
				t.Fatal(err)
			}
			select {
			case r := <-result:
				if r.err != nil {
					t.Fatal(r.err)
				}
				defer r.conn.CloseWithError(0, "")
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if retries == 0 || !server.HandshakeConfirmed() {
				t.Fatal("Retry admission not exercised")
			}
		})
	}
}
