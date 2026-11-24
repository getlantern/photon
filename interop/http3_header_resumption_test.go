package interop

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

// Keep the peer early-data policy fixed while changing the local receive budget.
func TestHTTP3TransportHeaderLimitResumption(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, policy := range []struct {
			name                              string
			seedLimit, currentLimit           int
			legacySeed, directSeed, wantEarly bool
		}{
			{name: "same", seedLimit: 2048, currentLimit: 2048, wantEarly: true},
			{name: "increase", seedLimit: 2048, currentLimit: 4096},
			{name: "decrease", seedLimit: 4096, currentLimit: 2048},
			{name: "default-equivalent", currentLimit: 10 << 20, wantEarly: true},
			{name: "legacy-equivalent", seedLimit: 1024, currentLimit: 1024, legacySeed: true, wantEarly: true},
			{name: "direct-to-pooled", directSeed: true},
			{name: "direct-equivalent", directSeed: true, currentLimit: 65536, wantEarly: true},
		} {
			t.Run(fmt.Sprintf("%x/%s", v, policy.name), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer cancel()
				ct, st := tlsConfigs(t)
				st.NextProtos = []string{"h3"}
				cache := &observedSessionCache{ClientSessionCache: tls.NewLRUClientSessionCache(4), stored: make(chan bool, 8)}
				ct.ClientSessionCache = cache
				socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				arrival := &earlyArrivalObserver{PacketConn: socket, seen: make(chan struct{})}
				// Early data is expected, so hold the resumed handshake until some arrives:
				// a client that is slow to write could otherwise finish the handshake first
				// and send nothing early at all. The first 0-RTT packet of any kind releases
				// it, so this guarantees only that early data was sent. A client that sends
				// none is released after two seconds, to fail the early-evidence check.
				if policy.wantEarly {
					var hellos atomic.Int32
					st.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
						if hellos.Add(1) > 1 {
							select {
							case <-arrival.seen:
							case <-time.After(2 * time.Second):
							case <-ctx.Done():
							}
						}
						return nil, nil
					}
				}
				transport := &upstream.Transport{Conn: arrival}
				defer func() { socket.Close(); transport.Close() }()
				listener, err := transport.ListenEarly(st, &upstream.Config{Allow0RTT: true, Versions: []upstream.Version{upstream.Version(v)}})
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				peers := make(chan *upstream.Conn, 2)
				server := &peerh3.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					settings := w.(peerh3.Settingser)
					select {
					case <-settings.ReceivedSettings():
					case <-r.Context().Done():
						return
					}
					if r.URL.Path == "/boundary" {
						w.Header().Set("X-Limit", strings.Repeat("x", 3000))
					}
					io.WriteString(w, "resumed header policy")
				})}
				done := make(chan struct{})
				go func() {
					defer close(done)
					for i := 0; i < 2; i++ {
						peer, err := listener.Accept(ctx)
						if err != nil {
							return
						}
						peers <- peer
						server.ServeQUICConn(peer)
					}
				}()
				defer func() { server.Close(); listener.Close(); <-done }()
				defer cancel() // first: a held handshake must not delay the teardown above
				origin := "https://" + socket.LocalAddr().String()
				config := &quic.Config{Versions: []quic.Version{v}, TokenStore: quic.NewLRUTokenStore(4, 4)}
				for round := 0; round < 2; round++ {
					var client interface {
						http.RoundTripper
						io.Closer
					}
					if round == 0 && policy.directSeed {
						client, err = ownh3.DialEarly(ctx, origin, ct, config, nil)
						if err != nil {
							t.Fatal(err)
						}
					} else {
						limit := policy.currentLimit
						if round == 0 {
							limit = policy.seedLimit
						}
						pooled := &ownh3.Transport{Enable0RTT: true, TLSClientConfig: ct, QUICConfig: config, MaxResponseHeaderBytes: limit}
						if round == 0 && policy.legacySeed {
							pooled.MaxResponseHeaderBytes = 0
							pooled.ClientConfig = &ownh3.ClientConfig{MaxHeaderBytes: limit}
						}
						client = pooled
					}
					defer client.Close()
					request, _ := http.NewRequestWithContext(ctx, ownh3.MethodGet0RTT, origin, nil)
					response, err := client.RoundTrip(request)
					if err != nil {
						t.Fatal(err, ";", arrival.summary())
					}
					body, err := io.ReadAll(response.Body)
					response.Body.Close()
					if err != nil || string(body) != "resumed header policy" || response.TLS == nil || response.TLS.DidResume != (round == 1) {
						t.Fatalf("round=%d body=%q TLS=%+v err=%v", round, body, response.TLS, err)
					}
					var peer *upstream.Conn
					select {
					case peer = <-peers:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					waitEarlyHandshake(t, ctx, peer.HandshakeComplete())
					wantEarly := round == 1 && policy.wantEarly
					state := peer.ConnectionState()
					if state.Used0RTT != wantEarly || state.TLS.DidResume != (round == 1) {
						t.Fatalf("round=%d early=%v resume=%v", round, state.Used0RTT, state.TLS.DidResume)
					}
					if round == 0 {
						waitEarlyTicket(t, ctx, cache)
					}
					if round == 1 && (policy.name == "increase" || policy.name == "decrease") {
						request, _ := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/boundary", nil)
						response, err := client.RoundTrip(request)
						if policy.name == "decrease" {
							if response != nil {
								response.Body.Close()
								t.Fatal("response exceeded current limit")
							}
							var protocol *ownh3.Error
							if !errors.As(err, &protocol) || protocol.ErrorCode != ownh3.ErrCodeExcessiveLoad || protocol.Remote {
								t.Fatalf("current receive limit not enforced: %v", err)
							}
						} else {
							if err != nil {
								t.Fatal("retained stale lower receive limit", err)
							}
							body, err := io.ReadAll(response.Body)
							response.Body.Close()
							if err != nil || string(body) != "resumed header policy" || len(response.Header.Get("X-Limit")) != 3000 {
								t.Fatal("resumed response incomplete", err)
							}
						}
					}

					client.Close()
				}
				select {
				case <-arrival.seen:
					if !policy.wantEarly {
						t.Fatal("changed receive limit still sent 0-RTT")
					}
				default:
					if policy.wantEarly {
						t.Fatal("missing early packet evidence;", arrival.summary())
					}
				}
			})
		}
	}
}
