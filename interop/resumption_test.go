package interop

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

type observedSessionCache struct {
	tls.ClientSessionCache
	stored chan bool
}

func (c *observedSessionCache) Put(key string, session *tls.ClientSessionState) {
	c.ClientSessionCache.Put(key, session)
	if session == nil {
		return
	}
	_, state, err := session.ResumptionState()
	if err == nil && state != nil {
		select {
		case c.stored <- state.EarlyData:
		default:
		}
	}
}

func TestClientResumptionUpstream(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", v), func(t *testing.T) {
			clientTLS, serverTLS := tlsConfigs(t)
			cache := &observedSessionCache{ClientSessionCache: tls.NewLRUClientSessionCache(8), stored: make(chan bool, 8)}
			clientTLS.ClientSessionCache = cache
			l, err := upstream.ListenAddr("127.0.0.1:0", serverTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version1, upstream.Version2}, Allow0RTT: true})
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for round := 0; round < 5; round++ {
				// The third connection explicitly disables tickets and must do a full
				// handshake despite the populated caller-owned cache.
				cfg := clientTLS.Clone()
				cfg.SessionTicketsDisabled = round == 2
				version := v
				if round >= 3 {
					version = quic.Version1
					if version == v {
						version = quic.Version2
					}
				}
				c, err := quic.DialAddr(ctx, l.Addr().String(), cfg, &quic.Config{Versions: []quic.Version{version}})
				if err != nil {
					t.Fatal(err)
				}
				defer c.CloseWithError(0, "")
				peer, err := l.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer peer.CloseWithError(0, "")
				want := round == 1 || round == 4
				if c.ConnectionState().TLS.DidResume != want || peer.ConnectionState().TLS.DidResume != want {
					t.Fatalf("round %d client/server resume=%t/%t", round, c.ConnectionState().TLS.DidResume, peer.ConnectionState().TLS.DidResume)
				}
				if peer.ConnectionState().Used0RTT {
					t.Fatal("client offered early data")
				}
				if len(c.ConnectionState().TLS.VerifiedChains) == 0 {
					t.Fatal("missing authenticated identity")
				}
				tx, err := c.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				tx.SetWriteDeadline(time.Now().Add(time.Second))
				if _, err := tx.Write([]byte("resumed application data")); err != nil {
					t.Fatal(err)
				}
				if err := tx.Close(); err != nil {
					t.Fatal(err)
				}
				rx, err := peer.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				rx.SetReadDeadline(time.Now().Add(time.Second))
				if b, err := io.ReadAll(rx); err != nil || string(b) != "resumed application data" {
					t.Fatalf("%q: %v", b, err)
				}
				if round != 2 {
					select {
					case early := <-cache.stored:
						if early {
							t.Fatal("stored ticket enables early data")
						}
					case <-ctx.Done():
						t.Fatal("no ticket received", ctx.Err())
					}
				}
				c.CloseWithError(0, "")
				peer.CloseWithError(0, "")
			}
			if clientTLS.ClientSessionCache != cache || clientTLS.SessionTicketsDisabled {
				t.Fatal("mutated caller TLS configuration")
			}
		})
	}
}

func TestServerResumptionUpstream(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", v), func(t *testing.T) {
			clientTLS, serverTLS := tlsConfigs(t)
			cache := &observedSessionCache{ClientSessionCache: tls.NewLRUClientSessionCache(8), stored: make(chan bool, 8)}
			clientTLS.ClientSessionCache = cache
			l, err := quic.ListenAddr("127.0.0.1:0", serverTLS, &quic.Config{Versions: []quic.Version{v}})
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for round := 0; round < 3; round++ {
				// The third connection explicitly disables tickets and must do a full
				// handshake despite the populated caller-owned cache.
				cfg := clientTLS.Clone()
				cfg.SessionTicketsDisabled = round == 2
				c, err := dialUpstreamAddr(t, ctx, l.Addr().String(), cfg, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}})
				if err != nil {
					t.Fatalf("round %d: dial: %v", round, err)
				}
				defer c.CloseWithError(0, "")
				peer, err := l.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer peer.CloseWithError(0, "")
				want := round == 1
				if c.ConnectionState().TLS.DidResume != want || peer.ConnectionState().TLS.DidResume != want {
					t.Fatalf("round %d client/server resume=%t/%t", round, c.ConnectionState().TLS.DidResume, peer.ConnectionState().TLS.DidResume)
				}
				if c.ConnectionState().Used0RTT {
					t.Fatal("client offered early data")
				}
				if len(c.ConnectionState().TLS.VerifiedChains) == 0 {
					t.Fatal("missing authenticated identity")
				}
				tx, err := c.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				tx.SetWriteDeadline(time.Now().Add(time.Second))
				if _, err := tx.Write([]byte("resumed application data")); err != nil {
					t.Fatal(err)
				}
				if err := tx.Close(); err != nil {
					t.Fatal(err)
				}
				rx, err := peer.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				rx.SetReadDeadline(time.Now().Add(time.Second))
				if b, err := io.ReadAll(rx); err != nil || string(b) != "resumed application data" {
					t.Fatalf("%q: %v", b, err)
				}
				if round < 2 {
					select {
					case early := <-cache.stored:
						if early {
							t.Fatal("stored ticket enables early data")
						}
					case <-ctx.Done():
						t.Fatal("no ticket received", ctx.Err())
					}
				}
				c.CloseWithError(0, "")
				peer.CloseWithError(0, "")
			}
			if clientTLS.ClientSessionCache != cache || clientTLS.SessionTicketsDisabled {
				t.Fatal("mutated caller TLS configuration")
			}
		})
	}
}
