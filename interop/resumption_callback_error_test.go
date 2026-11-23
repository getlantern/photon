package interop

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

func TestServerTicketCallbackFailureIsolation(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, operation := range []string{"wrap", "unwrap"} {
			t.Run(fmt.Sprintf("%x/%s", v, operation), func(t *testing.T) {
				clientTLS, serverTLS := tlsConfigs(t)
				cache := &sharedTicketCache{stored: make(chan struct{}, 1)}
				clientTLS.ClientSessionCache = cache
				var reject atomic.Bool
				var failures atomic.Int32
				injected := errors.New("test ticket callback failure")
				serverTLS.WrapSession = func(cs tls.ConnectionState, ss *tls.SessionState) ([]byte, error) {
					if operation == "wrap" && reject.Load() {
						failures.Add(1)
						return nil, injected
					}
					return serverTLS.EncryptTicket(cs, ss)
				}
				serverTLS.UnwrapSession = func(identity []byte, cs tls.ConnectionState) (*tls.SessionState, error) {
					if operation == "unwrap" && reject.Load() {
						failures.Add(1)
						return nil, injected
					}
					return serverTLS.DecryptTicket(identity, cs)
				}
				l, err := quic.ListenAddr("127.0.0.1:0", serverTLS, &quic.Config{Versions: []quic.Version{v}, EnableDatagrams: true})
				if err != nil {
					t.Fatal(err)
				}
				defer l.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				cfg := &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}, EnableDatagrams: true}
				for phase := 0; phase < 3; phase++ {
					reject.Store(phase == 1)
					c, err := dialUpstreamAddr(t, ctx, l.Addr().String(), clientTLS, cfg)
					if c != nil {
						defer c.CloseWithError(0, "")
					}
					if phase == 1 {
						if err == nil {
							select {
							case <-c.Context().Done():
								err = context.Cause(c.Context())
							case <-ctx.Done():
								t.Fatal("callback failure left connection alive", ctx.Err())
							}
						}
						var transportErr *upstream.TransportError
						if !errors.As(err, &transportErr) || !transportErr.Remote {
							t.Fatal("missing peer transport close", err)
						}
						if failures.Load() != 1 {
							t.Fatal("callback not exercised exactly once", failures.Load())
						}
						waiting, stop := context.WithTimeout(ctx, 50*time.Millisecond)
						accepted, acceptErr := l.Accept(waiting)
						stop()
						if accepted != nil || !errors.Is(acceptErr, context.DeadlineExceeded) {
							t.Fatal("failed connection exposed to application", acceptErr)
						}
						continue
					}
					if err != nil {
						t.Fatal(err)
					}
					peer, err := l.Accept(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if c.ConnectionState().TLS.DidResume != (phase == 2) || peer.ConnectionState().TLS.DidResume != (phase == 2) {
						t.Fatal("unexpected resumption after callback recovery")
					}
					if err := c.SendDatagram([]byte("listener survived")); err != nil {
						t.Fatal(err)
					}
					if b, err := peer.ReceiveDatagram(ctx); err != nil || string(b) != "listener survived" {
						t.Fatalf("%q: %v", b, err)
					}
					if phase == 0 {
						select {
						case <-cache.stored:
						case <-ctx.Done():
							t.Fatal("missing seed ticket", ctx.Err())
						}
						cache.freeze()
					}
					c.CloseWithError(0, "")
				}
			})
		}
	}
}
