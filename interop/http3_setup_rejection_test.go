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
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
)

// A small remembered stream window blocks initial HTTP SETTINGS. The resumed
// TLS callback is gated until an actual early packet arrives; rejection then
// interrupts OpenLocal before DialEarly can expose a ClientConn.
func TestHTTP3DialEarlySetupRejection(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			ct, st := tlsConfigs(t)
			st.NextProtos = []string{"h3"}
			cache := &observedSessionCache{ClientSessionCache: tls.NewLRUClientSessionCache(4), stored: make(chan bool, 8)}
			ct.ClientSessionCache = cache
			gate := make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(gate) }) }
			defer release()
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
			t.Cleanup(func() { socket.Close(); transport.Close() })
			origin := "https://" + socket.LocalAddr().String()
			for round := 0; round < 2; round++ {
				trace := &receiveWindowTrace{params: make(chan qlog.ParametersSet, 1)}
				config := &upstream.Config{Allow0RTT: round == 0, Versions: []upstream.Version{upstream.Version(version)}, Tracer: func(context.Context, bool, upstream.ConnectionID) qlogwriter.Trace { return trace }}
				if round == 0 {
					config.InitialStreamReceiveWindow = 1
					config.MaxStreamReceiveWindow = config.InitialStreamReceiveWindow
					t.Logf("remembered stream window: %d", config.InitialStreamReceiveWindow)
				}
				listener, err := transport.ListenEarly(st, config)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { listener.Close() })
				var requests atomic.Int32
				server := &peerh3.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); w.Write([]byte("setup recovered")) })}
				served := make(chan struct{})
				go func() {
					defer close(served)
					peer, err := listener.Accept(ctx)
					if err == nil {
						server.ServeQUICConn(peer)
					}
				}()
				t.Cleanup(func() { server.Close(); listener.Close(); <-served })
				type result struct {
					client *ownh3.ClientConn
					err    error
				}
				dialed := make(chan result, 1)
				go func() {
					client, err := ownh3.DialEarly(ctx, origin, ct, &quic.Config{Versions: []quic.Version{version}}, nil)
					dialed <- result{client, err}
				}()
				if round == 1 {
					select {
					case <-arrival.seen:
					case <-ctx.Done():
						t.Fatal("no partial early control packet", ctx.Err())
					}
					select {
					case got := <-dialed:
						if got.client != nil {
							got.client.Close()
						}
						t.Fatal("DialEarly escaped blocked setup", got.err)
					default:
					}
					if requests.Load() != 0 {
						t.Fatal("application request sent during setup")
					}
					release()
				}
				var client *ownh3.ClientConn
				select {
				case got := <-dialed:
					client, err = got.client, got.err
				case <-ctx.Done():
					t.Fatal("setup did not finish", ctx.Err())
				}
				if err != nil {
					t.Fatal("DialEarly failed to recover setup rejection", err)
				}
				t.Cleanup(func() { client.Close() })
				// Recovery was internal: callers see a usable ordinary HTTP generation.
				next, err := client.NextConnection(ctx)
				if err != nil || next != client {
					t.Fatal("extra recovery required", err)
				}
				request, _ := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/", nil)
				response, err := client.RoundTrip(request)
				if err != nil {
					t.Fatalf("round=%d peer-key-phase=%d: %v", round, trace.phase.Load(), err)
				}
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil || string(body) != "setup recovered" {
					t.Fatal(string(body), err)
				}
				if response.TLS == nil || response.TLS.DidResume != (round == 1) {
					t.Fatal("resumption state")
				}
				if requests.Load() != 1 {
					t.Fatal("unexpected HTTP request count", requests.Load())
				}
				if round == 0 {
					waitEarlyTicket(t, ctx, cache)
				}
				client.Close()
				server.Close()
				listener.Close()
				<-served
			}
		})
	}
}
