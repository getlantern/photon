package interop

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

// The independent server always permits early data. Changes to client-local
// additional settings must suppress the offer while preserving TLS resumption.
func TestHTTP3ClientAdditionalSettingsResumption(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, mode := range []string{"direct", "pooled"} {
			for _, policy := range []string{"same", "changed", "added", "removed", "empty"} {
				t.Run(fmt.Sprintf("%x/%s/%s", v, mode, policy), func(t *testing.T) {
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
					if policy == "same" {
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
					observed := make(chan map[uint64]uint64, 2)
					server := &peerh3.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						settings := w.(peerh3.Settingser)
						select {
						case <-settings.ReceivedSettings():
						case <-r.Context().Done():
							return
						}
						observed <- maps.Clone(settings.Settings().Other)
						io.WriteString(w, "resumed extras")
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
						extra := map[uint64]uint64{0xface: 7, 0x21: 9}
						if round == 1 {
							switch policy {
							case "changed":
								extra[0xface] = 8
							case "added":
								extra[0x22] = 0
							case "removed":
								delete(extra, 0xface)
							case "empty":
								extra = nil
							}
						}
						var client interface {
							http.RoundTripper
							io.Closer
						}
						if mode == "direct" {
							client, err = ownh3.DialEarly(ctx, origin, ct, config, &ownh3.ClientConfig{AdditionalSettings: extra})
							if err != nil {
								t.Fatal(err, ";", arrival.summary())
							}
						} else {
							client = &ownh3.Transport{Enable0RTT: true, TLSClientConfig: ct, QUICConfig: config, AdditionalSettings: extra}
						}
						defer client.Close()
						request, _ := http.NewRequestWithContext(ctx, ownh3.MethodGet0RTT, origin, nil)
						response, err := client.RoundTrip(request)
						if err != nil {
							t.Fatal(err, ";", arrival.summary())
						}
						body, err := io.ReadAll(response.Body)
						response.Body.Close()
						if err != nil || string(body) != "resumed extras" || response.TLS == nil || response.TLS.DidResume != (round == 1) {
							t.Fatalf("round=%d body=%q TLS=%+v err=%v", round, body, response.TLS, err)
						}
						var peer *upstream.Conn
						select {
						case peer = <-peers:
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
						waitEarlyHandshake(t, ctx, peer.HandshakeComplete())
						wantEarly := round == 1 && policy == "same"
						state := peer.ConnectionState()
						if state.Used0RTT != wantEarly || state.TLS.DidResume != (round == 1) {
							t.Fatalf("round=%d early=%v resume=%v; %s", round, state.Used0RTT, state.TLS.DidResume, arrival.summary())
						}
						select {
						case got := <-observed:
							for _, id := range []uint64{0xface, 0x21, 0x22} {
								value, present := got[id]
								want, wantPresent := extra[id]
								if value != want || present != wantPresent {
									t.Fatalf("setting %x=%d/%v want %d/%v", id, value, present, want, wantPresent)
								}
							}
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
						if round == 0 {
							waitEarlyTicket(t, ctx, cache)
						}
						client.Close()
					}
					select {
					case <-arrival.seen:
						if policy != "same" {
							t.Fatal("changed client settings still sent 0-RTT")
						}
					default:
						if policy == "same" {
							t.Fatal("missing early packet evidence;", arrival.summary())
						}
					}
				})
			}
		}
	}
}
