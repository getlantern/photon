package interop

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/token"
	engine "github.com/getlantern/photon/internal/transport"
	"github.com/getlantern/photon/internal/wire"
	upstream "github.com/quic-go/quic-go"
)

// Advance only the admission clock: no sleep is needed to expire the Retry,
// and the independent client's handshake deadline remains a failure guard.
func TestStatelessInvalidTokenUpstream(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprintf("%x", v), func(t *testing.T) {
			ct, st := tlsConfigs(t)
			socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer socket.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			codec, err := token.NewRetryTokens([32]byte{7}, nil, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			endpoint, err := engine.NewServerEndpoint(engine.Config{Version: v, TLS: st, Parameters: wire.DefaultTransportParameters()}, codec, 1)
			if err != nil {
				t.Fatal(err)
			}
			defer endpoint.Close()
			result := make(chan error, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				c, err := dialUpstreamAddr(t, ctx, socket.LocalAddr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}})
				if c != nil {
					c.CloseWithError(0, "unexpected handshake")
				}
				result <- err
			}()
			defer func() { cancel(); <-done }()
			deadline, _ := ctx.Deadline()
			if err := socket.SetReadDeadline(deadline); err != nil {
				t.Fatal(err)
			}
			buffer := make([]byte, 65535)
			sentClose := false
			retries := 0
			for !sentClose {
				n, peer, err := socket.ReadFromUDP(buffer)
				if err != nil {
					t.Fatal(err)
				}
				h, err := wire.ParseHeader(buffer[:n], 0)
				if err != nil || h.Type != wire.Initial {
					t.Fatal("expected Initial", err)
				}
				now := time.Unix(100, 0)
				if len(h.Token) > 0 {
					now = now.Add(time.Second)
				}
				c, out, err := endpoint.HandleDatagram(ctx, buffer[:n], peer.AddrPort(), now)
				if c != nil {
					t.Fatal("invalid token allocated connection")
				}
				if err != nil || out == nil || out.Conn != nil {
					t.Fatal("stateless response", err)
				}
				if len(out.Data) > n || out.Peer != peer.AddrPort() {
					t.Fatal("response budget or routing")
				}
				response, err := wire.ParseHeader(out.Data, 0)
				if err != nil {
					t.Fatal(err)
				}
				switch response.Type {
				case wire.Retry:
					retries++
				case wire.Initial:
					if len(h.Token) == 0 {
						t.Fatal("close before token")
					}
					sentClose = true
				default:
					t.Fatal("unexpected response", response.Type)
				}
				if _, err := socket.WriteToUDP(out.Data, peer); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case err := <-result:
				var closed *upstream.TransportError
				if !errors.As(err, &closed) || !closed.Remote || closed.ErrorCode != 0xb {
					t.Fatalf("expected remote INVALID_TOKEN, got %v", err)
				}
			case <-ctx.Done():
				t.Fatal("client did not receive close", ctx.Err())
			}
			if retries == 0 {
				t.Fatal("Retry not exercised")
			}
			if c, _, err := endpoint.Accept(); c != nil || err == nil {
				t.Fatal("stateless response became accepted connection", err)
			}
		})
	}
}
