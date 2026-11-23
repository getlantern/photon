package interop

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

// A callback may return a resumed early connection independently of the HTTP
// policy and cache. Neither case authorizes sending HTTP streams before TLS.
func TestHTTP3CustomDialWaitsForHandshake(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, accept := range []bool{false, true} {
			for _, bypassCache := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/accept=%v/bypass-cache=%v", version, accept, bypassCache), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					ct, st := tlsConfigs(t)
					st.NextProtos = []string{"h3"}
					cache := &observedSessionCache{ClientSessionCache: tls.NewLRUClientSessionCache(4), stored: make(chan bool, 8)}
					if !bypassCache {
						ct.ClientSessionCache = cache
					}
					gate := make(chan struct{})
					var once sync.Once
					release := func() { once.Do(func() { close(gate) }) }
					// Release before server cleanup even on failure behind the TLS gate.
					var hellos atomic.Int32
					st.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
						if hellos.Add(1) > 1 {
							select {
							case <-gate:
							case <-ctx.Done():
							}
						}
						return nil, nil
					}
					socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					arrival := &earlyArrivalObserver{PacketConn: socket, seen: make(chan struct{})}
					transport := &upstream.Transport{Conn: arrival}
					defer func() { release(); transport.Close(); socket.Close() }()
					origin := "https://" + socket.LocalAddr().String()
					connected := make(chan *quic.Conn, 2)
					pool := &ownh3.Transport{TLSClientConfig: ct, Enable0RTT: true, QUICConfig: &quic.Config{Versions: []quic.Version{version}},
						Dial: func(ctx context.Context, address string, tc *tls.Config, qc *quic.Config) (*quic.Conn, error) {
							if bypassCache {
								tc.ClientSessionCache = cache
							}
							conn, err := quic.DialAddrEarly(ctx, address, tc, qc)
							if err == nil {
								connected <- conn
							}
							return conn, err
						}}
					defer pool.Close()
					for round := 0; round < 2; round++ {
						func() {
							listener, err := transport.ListenEarly(st, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, Allow0RTT: round == 0 || accept})
							if err != nil {
								t.Fatal(err)
							}
							var handled atomic.Int32
							server := &peerh3.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { handled.Add(1); io.WriteString(w, "after handshake") })}
							done := make(chan struct{})
							go func() { defer close(done); server.ServeListener(listener) }()
							defer func() {
								if t.Failed() {
									release()
								}
								server.Close()
								listener.Close()
								select {
								case <-done:
								case <-time.After(time.Second):
									t.Error("server did not join")
								}
							}()
							request, _ := http.NewRequestWithContext(ctx, ownh3.MethodGet0RTT, origin+"/", nil)
							type result struct {
								response *http.Response
								err      error
							}
							outcome := make(chan result, 1)
							go func() { response, err := pool.RoundTrip(request); outcome <- result{response, err} }()
							var conn *quic.Conn
							select {
							case conn = <-connected:
							case <-ctx.Done():
								t.Fatal("custom dial did not return", ctx.Err())
							}
							if round == 1 {
								select {
								case <-conn.HandshakeComplete():
									t.Fatal("fixture did not return an early connection")
								default:
								}
								select {
								case got := <-outcome:
									if got.response != nil {
										got.response.Body.Close()
									}
									t.Fatal("HTTP setup finished before handshake", got.err)
								case <-arrival.seen:
									t.Fatal("unexpected 0-RTT packet while HTTP setup must wait")
								case <-time.After(20 * time.Millisecond):
								}
								if handled.Load() != 0 {
									t.Fatal("application ran behind TLS gate")
								}
								release()
							}
							select {
							case got := <-outcome:
								if got.err != nil {
									t.Fatal(got.err)
								}
								body, err := io.ReadAll(got.response.Body)
								got.response.Body.Close()
								if err != nil || got.response.StatusCode != 200 || string(body) != "after handshake" || got.response.TLS == nil || got.response.TLS.DidResume != (round == 1) {
									t.Fatal("response", string(body), err, got.response.TLS)
								}
							case <-ctx.Done():
								t.Fatal("request did not finish", ctx.Err())
							}
							if handled.Load() != 1 {
								t.Fatal("request duplicated", handled.Load())
							}
							if round == 0 {
								waitEarlyTicket(t, ctx, cache)
							}
							pool.CloseIdleConnections()
						}()
						// No pool worker survives CloseIdleConnections. For the regular HTTP
						// cache, keep its remembered SETTINGS but turn off early HTTP admission.
						// The bypass case leaves admission enabled with no remembered SETTINGS.
						if !bypassCache {
							pool.Enable0RTT = false
						}
					}
				})
			}
		}
	}
}
