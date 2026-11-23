package interop

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/transport"
	upstream "github.com/quic-go/quic-go"
)

func TestKeepAliveUpstream(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, server := range []bool{false, true} {
			for _, period := range []time.Duration{0, time.Millisecond, 10 * time.Second} {
				enabled := period != 0
				t.Run(fmt.Sprintf("%x/server=%t/period=%s", version, server, period), func(t *testing.T) {
					t.Parallel()
					clientTLS, serverTLS := tlsConfigs(t)
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					cfg := &quic.Config{Versions: []quic.Version{version}, MaxIdleTimeout: 5 * time.Second, DisablePathMTUDiscovery: true}
					if enabled {
						cfg.KeepAlivePeriod = period
					}
					peerCfg := &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, MaxIdleTimeout: time.Second, DisablePathMTUDiscovery: true}
					var receiver *quic.Conn
					var sender *upstream.Conn
					if server {
						l, err := quic.ListenAddr("127.0.0.1:0", serverTLS, cfg)
						if err != nil {
							t.Fatal(err)
						}
						defer l.Close()
						sender, err = dialUpstreamAddr(t, ctx, l.Addr().String(), clientTLS, peerCfg)
						if err != nil {
							t.Fatal(err)
						}
						defer sender.CloseWithError(0, "")
						receiver, err = l.Accept(ctx)
						if err != nil {
							t.Fatal(err)
						}
					} else {
						l, err := upstream.ListenAddr("127.0.0.1:0", serverTLS, peerCfg)
						if err != nil {
							t.Fatal(err)
						}
						defer l.Close()
						receiver, err = quic.DialAddr(ctx, l.Addr().String(), clientTLS, cfg)
						if err != nil {
							t.Fatal(err)
						}
						defer receiver.CloseWithError(0, "")
						sender, err = l.Accept(ctx)
						if err != nil {
							t.Fatal(err)
						}
						defer sender.CloseWithError(0, "")
					}

					exchange := func() {
						t.Helper()
						tx, err := receiver.OpenUniStreamSync(ctx)
						if err != nil {
							t.Fatal(err)
						}
						tx.SetWriteDeadline(time.Now().Add(time.Second))
						if _, err := tx.Write([]byte("keepalive interoperability")); err != nil {
							t.Fatal(err)
						}
						if err := tx.Close(); err != nil {
							t.Fatal(err)
						}
						rx, err := sender.AcceptUniStream(ctx)
						if err != nil {
							t.Fatal(err)
						}
						rx.SetReadDeadline(time.Now().Add(time.Second))
						if b, err := io.ReadAll(rx); err != nil || string(b) != "keepalive interoperability" {
							t.Fatalf("payload %q: %v", b, err)
						}
					}
					exchange()
					if !enabled {
						select {
						case <-sender.Context().Done():
							var timeout *upstream.IdleTimeoutError
							if !errors.As(context.Cause(sender.Context()), &timeout) {
								t.Fatal("upstream close was not idle timeout", context.Cause(sender.Context()))
							}
						case <-ctx.Done():
							t.Fatal("disabled upstream did not expire", ctx.Err())
						}
						select {
						case <-receiver.Context().Done():
							if !errors.Is(context.Cause(receiver.Context()), transport.ErrIdleTimeout) {
								t.Fatal("independent close was not idle timeout", context.Cause(receiver.Context()))
							}
						case <-ctx.Done():
							t.Fatal("disabled independent peer did not expire", ctx.Err())
						}
						return
					}
					// The configured ten-second period must clamp to half the peer's one-
					// second timeout. Upstream keepalive and both peers' MTU discovery are off.
					timer := time.NewTimer(3500 * time.Millisecond)
					defer timer.Stop()
					select {
					case <-sender.Context().Done():
						t.Fatal("upstream expired with keepalive", context.Cause(sender.Context()))
					case <-receiver.Context().Done():
						t.Fatal("independent peer expired with keepalive", context.Cause(receiver.Context()))
					case <-timer.C:
					}
					exchange()
				})
			}
		}
	}
}
