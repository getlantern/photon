package interop

import (
	"context"
	"crypto/tls"
	"errors"
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

// Discard the resumed server flight until the HTTP handler has consumed the
// request. Returning success keeps the peer's sender running; loss recovery
// retransmits after release. No server TLS or application bytes can reach the
// client while held, so the request cannot be a hidden 1-RTT retransmission.
type httpEarlyFlightGate struct {
	net.PacketConn
	held    atomic.Bool
	dropped atomic.Int32
}

func (g *httpEarlyFlightGate) WriteTo(b []byte, address net.Addr) (int, error) {
	if g.held.Load() {
		g.dropped.Add(1)
		return len(b), nil
	}
	return g.PacketConn.WriteTo(b, address)
}

func TestHTTP3PublicEarlyRecovery(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, accept := range []bool{false, true} {
			for _, earlyMethod := range []string{ownh3.MethodGet0RTT, ownh3.MethodHead0RTT} {
				t.Run(fmt.Sprintf("%x/accept=%t/%s", version, accept, earlyMethod), func(t *testing.T) {
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
					flight := &httpEarlyFlightGate{PacketConn: socket}
					arrival := &earlyArrivalObserver{PacketConn: flight, seen: make(chan struct{})}
					tr := &upstream.Transport{Conn: arrival}
					t.Cleanup(func() { socket.Close(); tr.Close() })
					origin := "https://" + socket.LocalAddr().String()
					for round := 0; round < 2; round++ {
						flight.held.Store(round == 1 && accept)
						listener, err := tr.ListenEarly(st, &upstream.Config{Allow0RTT: round == 0 || accept, Versions: []upstream.Version{upstream.Version(version)}})
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { listener.Close() })
						requests := make(chan string, 8)
						dispatched := make(chan struct{}, 8)
						peerHandshake := make(chan (<-chan struct{}), 1)
						server := &peerh3.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							requests <- r.Method + " " + r.URL.Path
							dispatched <- struct{}{}
							w.Write([]byte("public early HTTP"))
						})}
						done := make(chan struct{})
						go func() {
							defer close(done)
							conn, err := listener.Accept(ctx)
							if err == nil {
								peerHandshake <- conn.HandshakeComplete()
								server.ServeQUICConn(conn)
							}
						}()
						t.Cleanup(func() { server.Close(); listener.Close(); <-done })
						dialCtx, cancelDial := context.WithCancel(ctx)
						client, err := ownh3.DialEarly(dialCtx, origin, ct, &quic.Config{Versions: []quic.Version{version}}, nil)
						cancelDial()
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { client.Close() })
						if round == 1 {
							cause := errors.New("caller stopped handshake wait")
							waitCtx, stopWait := context.WithTimeoutCause(ctx, 20*time.Millisecond, cause)
							next, waitErr := client.NextConnection(waitCtx)
							stopWait()
							if next != nil || !errors.Is(waitErr, cause) {
								t.Fatal("canceled recovery wait", next, waitErr)
							}
							// An ordinary method must not leave while the handshake
							// is gated, even though the HTTP session is early-ready.
							waitCtx, stopWait = context.WithTimeout(ctx, 20*time.Millisecond)
							ordinary, _ := http.NewRequestWithContext(waitCtx, http.MethodPost, origin+"/must-wait", nil)
							_, waitErr = client.RoundTrip(ordinary)
							stopWait()
							if !errors.Is(waitErr, context.DeadlineExceeded) {
								t.Fatal("ordinary method did not wait", waitErr)
							}
						}
						method := http.MethodGet
						if round == 1 {
							method = earlyMethod
						}
						request, _ := http.NewRequestWithContext(ctx, method, origin+"/request", nil)
						type outcome struct {
							response *http.Response
							err      error
						}
						result := make(chan outcome, 1)
						go func() { response, err := client.RoundTrip(request); result <- outcome{response, err} }()
						if round == 1 {
							select {
							case <-arrival.seen:
							case <-ctx.Done():
								t.Fatal("no early wire packet", ctx.Err())
							}
							release()
							if accept {
								select {
								case <-dispatched:
								case <-ctx.Done():
									t.Fatal("HTTP request not delivered in 0-RTT", ctx.Err())
								}
								var handshake <-chan struct{}
								select {
								case handshake = <-peerHandshake:
								case <-ctx.Done():
									t.Fatal(ctx.Err())
								}
								select {
								case <-handshake:
									t.Fatal("handler ran after TLS completion")
								default:
								}
								if flight.dropped.Load() == 0 {
									t.Fatal("server flight gate did not discard output")
								}
								flight.held.Store(false)
							}
						}
						var got outcome
						select {
						case got = <-result:
						case <-ctx.Done():
							t.Fatal("request stuck", ctx.Err())
						}
						if round == 1 && !accept {
							if !errors.Is(got.err, quic.Err0RTTRejected) {
								t.Fatal("expected early rejection", got.err)
							}
							type recovered struct {
								client *ownh3.ClientConn
								err    error
							}
							recoveries := make(chan recovered, 8)
							for i := 0; i < cap(recoveries); i++ {
								go func() { next, err := client.NextConnection(ctx); recoveries <- recovered{next, err} }()
							}
							first := <-recoveries
							fresh, err := first.client, first.err
							for i := 1; i < cap(recoveries); i++ {
								next := <-recoveries
								if next.err != nil || next.client != fresh {
									t.Fatal("concurrent recovery", next.err)
								}
							}
							if err != nil || fresh == client {
								t.Fatal("reconstruction", err)
							}
							if client.Context() != fresh.Context() || client.Context().Err() != nil {
								t.Fatal("replacement changed or canceled transport context")
							}
							if err := client.CloseWithError(0x123, "stale generation"); err != nil {
								t.Fatal(err)
							}
							client.Close()
							again, err := client.NextConnection(ctx)
							if err != nil || again != fresh {
								t.Fatal("repeated recovery", err)
							}
							client = fresh
							t.Cleanup(func() { fresh.Close() })
							request, _ = http.NewRequestWithContext(ctx, http.MethodGet, origin+"/fresh", nil)
							got.response, got.err = client.RoundTrip(request)
						} else {
							if request.Method != method {
								t.Fatal("request method mutated")
							}
							same, err := client.NextConnection(ctx)
							if err != nil || same != client {
								t.Fatal("accepted/ordinary session replaced", err)
							}
						}
						if got.err != nil {
							t.Fatal(got.err)
						}
						body, err := io.ReadAll(got.response.Body)
						got.response.Body.Close()
						expectedBody := "public early HTTP"
						if round == 1 && accept && earlyMethod == ownh3.MethodHead0RTT {
							expectedBody = ""
						}
						if err != nil || string(body) != expectedBody {
							t.Fatal(string(body), err)
						}
						if got.response.TLS == nil || got.response.TLS.DidResume != (round == 1) {
							t.Fatal("resumption state")
						}
						expected := "GET /request"
						if round == 1 && accept && earlyMethod == ownh3.MethodHead0RTT {
							expected = "HEAD /request"
						}
						if round == 1 && !accept {
							expected = "GET /fresh"
						}
						select {
						case path := <-requests:
							if path != expected {
								t.Fatal(path)
							}
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
						select {
						case path := <-requests:
							t.Fatal("extra request", path)
						default:
						}
						if round == 0 {
							waitEarlyTicket(t, ctx, cache)
						}
						client.Close()
						server.Close()
						listener.Close()
						<-done
					}
				})
			}
		}
	}
}
