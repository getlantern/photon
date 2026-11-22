package interop

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/token"
	engine "github.com/getlantern/photon/internal/transport"
	"github.com/getlantern/photon/internal/wire"
	upstream "github.com/quic-go/quic-go"
)

// The reference client advertises reception; the independent engine advertises
// no reception capability. This validates asymmetric, real TLS negotiation for
// engine-level sending without enabling the unfinished public receive API.
func TestPartialResetEngineSenderUpstream(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, reliable := range []int{0, 256, 1500} {
			t.Run(fmt.Sprintf("%x/prefix=%d", version, reliable), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				var key [32]byte
				if _, err := rand.Read(key[:]); err != nil {
					socket.Close()
					t.Fatal(err)
				}
				codec, err := token.NewRetryTokens(key, nil, time.Minute)
				if err != nil {
					socket.Close()
					t.Fatal(err)
				}
				endpoint, err := engine.NewServerEndpoint(engine.Config{Version: version, TLS: st, Parameters: wire.DefaultTransportParameters()}, codec, 4)
				if err != nil {
					socket.Close()
					t.Fatal(err)
				}
				server, err := engine.NewUDPServer(socket, endpoint)
				if err != nil {
					endpoint.Close()
					socket.Close()
					t.Fatal(err)
				}
				defer server.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				peer, err := dialUpstreamAddr(t, ctx, server.Addr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, EnableStreamResetPartialDelivery: true})
				if err != nil {
					t.Fatal(err)
				}
				defer peer.CloseWithError(0, "")
				support := peer.ConnectionState().SupportsStreamResetPartialDelivery
				if !support.Local || support.Remote {
					t.Fatal("asymmetric capability", support)
				}
				conn, _, err := server.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				data := bytes.Repeat([]byte("0123456789"), 500)
				released := false
				var releasedError error
				if err := server.Do(ctx, func(*engine.ServerEndpoint) error {
					id, err := conn.OpenStream(true)
					if err != nil {
						return err
					}
					if err := conn.AutoReleaseStream(id, func(err error) { released = true; releasedError = err }); err != nil {
						return err
					}
					for offset := 0; offset < len(data); {
						n, err := conn.WriteStream(id, data[offset:], false)
						if err != nil {
							return err
						}
						if n == 0 {
							return io.ErrNoProgress
						}
						offset += n
					}
					return conn.CancelWriteAt(id, 7, uint64(reliable))
				}); err != nil {
					t.Fatal(err)
				}
				recv, err := peer.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				deadline, _ := ctx.Deadline()
				recv.SetReadDeadline(deadline)
				got, err := io.ReadAll(recv)
				var reset *upstream.StreamError
				if !errors.As(err, &reset) || reset.ErrorCode != 7 || !reset.Remote || !bytes.Equal(got, data[:reliable]) {
					t.Fatalf("read %d bytes reset=%v, want %d", len(got), err, reliable)
				}
				if err := server.Wait(ctx, func(*engine.ServerEndpoint) (bool, error) { return released, releasedError }); err != nil {
					t.Fatal("sender retirement", err)
				}
			})
		}
	}
}
