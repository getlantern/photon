package interop

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/token"
	engine "github.com/getlantern/photon/internal/transport"
	"github.com/getlantern/photon/internal/wire"
	upstream "github.com/quic-go/quic-go"
)

func TestUDPServerRuntime(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprintf("%x", uint32(v)), func(t *testing.T) {
			clientTLS, serverTLS := tlsConfigs(t)
			socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			var key [32]byte
			if _, err := rand.Read(key[:]); err != nil {
				t.Fatal(err)
			}
			codec, _ := token.NewRetryTokens(key, nil, time.Minute)
			params := wire.DefaultTransportParameters()
			params.MaxDatagramFrameSize = 65535
			endpoint, err := engine.NewServerEndpoint(engine.Config{Version: v, TLS: serverTLS, Parameters: params}, codec, 4)
			if err != nil {
				socket.Close()
				t.Fatal(err)
			}
			server, err := engine.NewUDPServer(socket, endpoint)
			if err != nil {
				socket.Close()
				t.Fatal(err)
			}
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			results := make(chan peerResult, 2)
			for i := 0; i < 2; i++ {
				go func() {
					c, err := dialUpstreamAddr(t, ctx, server.Addr().String(), clientTLS, &upstream.Config{EnableDatagrams: true, Versions: []upstream.Version{upstream.Version(v)}})
					if err == nil {
						err = c.SendDatagram([]byte("wake blocked receiver"))
					}
					results <- peerResult{c, err}
				}()
			}
			for i := 0; i < 2; i++ {
				c, peer, err := server.Accept(ctx)
				if err != nil || c == nil || !peer.IsValid() {
					t.Fatal("runtime accept", err)
				}
				var data []byte
				if err := server.Wait(ctx, func(*engine.ServerEndpoint) (bool, error) {
					var err error
					data, err = c.ReceiveDatagram()
					if errors.Is(err, stream.ErrWouldBlock) {
						return false, nil
					}
					return true, err
				}); err != nil {
					t.Fatal("blocked receive", err)
				}
				if string(data) != "wake blocked receiver" {
					t.Fatal("datagram contents", string(data))
				}
				if err := server.Do(ctx, func(*engine.ServerEndpoint) error {
					if !c.HandshakeConfirmed() {
						return errors.New("accepted unconfirmed connection")
					}
					return c.CloseWithError(42, "runtime test complete", time.Now())
				}); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < 2; i++ {
				select {
				case r := <-results:
					if r.err != nil {
						t.Fatal(r.err)
					}
					defer r.conn.CloseWithError(0, "")
					select {
					case <-r.conn.Context().Done():
						var app *upstream.ApplicationError
						if !errors.As(context.Cause(r.conn.Context()), &app) || app.ErrorCode != 42 {
							t.Fatal("wire close", context.Cause(r.conn.Context()))
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
		})
	}
}
