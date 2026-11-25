package interop

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

func TestHTTP3ServerEarlyRequestTLSState(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, allow := range []bool{true, false} {
			for _, useListener := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/allow=%t/listener=%t", v, allow, useListener), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					ct, st := tlsConfigs(t)
					ct.NextProtos = []string{"h3"}
					st.NextProtos = []string{"h3"}
					cache := &observedSessionCache{ClientSessionCache: tls.NewLRUClientSessionCache(4), stored: make(chan bool, 8)}
					ct.ClientSessionCache = cache
					tokens := &observedTokenStore{TokenStore: upstream.NewLRUTokenStore(4, 4), stored: make(chan struct{}, 4)}
					cfg := &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}, TokenStore: tokens}
					handled := make(chan *tls.ConnectionState, 2)
					policySeen := make(chan struct{}, 1)
					server := &ownh3.Server{AllowEarlyData: func(*http.Request) bool { policySeen <- struct{}{}; return allow }, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handled <- r.TLS; w.Write([]byte("early HTTP response")) })}
					defer server.Close()
					st, err := server.ConfigureTLSConfig(st)
					if err != nil {
						t.Fatal(err)
					}
					l, err := quic.ListenAddrEarly("127.0.0.1:0", st, &quic.Config{Versions: []quic.Version{v}, Allow0RTT: true})
					if err != nil {
						t.Fatal(err)
					}
					defer l.Close()
					seed, err := dialUpstreamAddr(t, ctx, l.Addr().String(), ct, cfg)
					if err != nil {
						t.Fatal(err)
					}
					defer seed.CloseWithError(0, "")
					if _, err := l.Accept(ctx); err != nil {
						t.Fatal(err)
					}
					waitEarlyTicket(t, ctx, cache)
					select {
					case <-tokens.stored:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					gate := &earlyPacketGate{PacketConn: socket, seen: make(chan struct{}), done: make(chan struct{})}
					release := make(chan struct{})
					flight := &heldServerFlight{earlyPacketGate: gate, release: release}
					tr := &upstream.Transport{Conn: flight}
					defer func() { gate.Close(); tr.Close() }()
					stop := context.AfterFunc(ctx, func() { gate.Close() })
					defer stop()
					c, err := tr.DialEarly(ctx, l.Addr(), ct, cfg)
					if err != nil {
						t.Fatal(err)
					}
					defer c.CloseWithError(0, "")
					peerTransport := &peerh3.Transport{Dial: loopbackDial(t)}
					defer peerTransport.Close()
					client := peerTransport.NewClientConn(c)
					accepted := make(chan *quic.Conn, 1)
					server.ConnContext = func(base context.Context, conn *quic.Conn) context.Context {
						accepted <- conn
						return base
					}
					served := make(chan error, 1)
					if useListener {
						go func() { served <- server.ServeListener(l) }()
					} else {
						peer, err := l.Accept(ctx)
						if err != nil {
							t.Fatal(err)
						}
						go func() { served <- server.ServeQUICConn(ctx, peer) }()
					}
					defer func() {
						server.Close()
						cleanup, stop := context.WithTimeout(context.Background(), time.Second)
						defer stop()
						if err := server.Shutdown(cleanup); err != nil {
							t.Error("HTTP workers did not stop", err)
						}
						select {
						case <-served:
						case <-time.After(time.Second):
							t.Error("server did not stop")
						}
					}()
					var peer *quic.Conn
					select {
					case peer = <-accepted:
					case err := <-served:
						t.Fatal("server exited before connection callback", err)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					if peer.ConnectionState().TLS.HandshakeComplete {
						t.Fatal("connection callback ran after held handshake completed")
					}
					origin := "https://" + l.Addr().String()
					request, err := http.NewRequestWithContext(ctx, peerh3.MethodGet0RTT, origin+"/early", nil)
					if err != nil {
						t.Fatal(err)
					}
					result := make(chan error, 1)
					go func() {
						response, err := client.RoundTrip(request)
						if err == nil {
							var b []byte
							b, err = io.ReadAll(response.Body)
							response.Body.Close()
							if string(b) != "early HTTP response" && err == nil {
								err = fmt.Errorf("wrong early response")
							}
						}
						result <- err
					}()
					if !allow {
						select {
						case <-policySeen:
						case <-ctx.Done():
							t.Fatal("resource policy not consulted", ctx.Err())
						}
						select {
						case <-handled:
							t.Fatal("resource ran before TLS without approval")
						default:
						}
						close(release)
					}
					var initial *tls.ConnectionState
					select {
					case initial = <-handled:
					case err := <-served:
						t.Fatal("server exited", err)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					if initial == nil || initial.HandshakeComplete != !allow {
						t.Fatal("resource policy did not control dispatch timing")
					}
					if flight.retries.Load() != 0 {
						t.Fatal("unexpected Retry")
					}
					if allow {
						close(release)
					}
					select {
					case err := <-result:
						if err != nil {
							t.Fatal(err)
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					waitEarlyHandshake(t, ctx, peer.HandshakeComplete())
					request, err = http.NewRequestWithContext(ctx, http.MethodGet, origin+"/established", nil)
					if err != nil {
						t.Fatal(err)
					}
					response, err := client.RoundTrip(request)
					if err != nil {
						t.Fatal(err)
					}
					body, readErr := io.ReadAll(response.Body)
					closeErr := response.Body.Close()
					if readErr != nil || closeErr != nil || string(body) != "early HTTP response" {
						t.Fatalf("established response %q: read=%v close=%v", body, readErr, closeErr)
					}
					select {
					case state := <-handled:
						if state == nil || !state.HandshakeComplete || state.NegotiatedProtocol != "h3" {
							t.Fatal("stale request TLS state")
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					if initial.HandshakeComplete != !allow {
						t.Fatal("old request TLS state mutated")
					}
					if !peer.ConnectionState().Used0RTT {
						t.Fatal("early data not accepted")
					}
					if !peer.ConnectionState().TLS.DidResume || !c.ConnectionState().TLS.DidResume {
						t.Fatal("early request did not use TLS resumption on both peers")
					}
				})
			}
		}
	}
}
