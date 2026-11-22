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
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

// The raw APIs never use 0-RTT. On a resumed connection whose handshake cannot
// finish (the server's flight is discarded), OpenRequestStream and the
// Transport constructors must wait, give up with their context, and put no
// request in front of the server. The first round only obtains the ticket.
func TestHTTP3RawAPIsWaitForHandshake(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, mode := range []string{"DialEarly", "NewClientConn", "NewRawClientConn"} {
			t.Run(fmt.Sprintf("%x/%s", version, mode), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				ct, st := tlsConfigs(t)
				ct.NextProtos = []string{"h3"}
				st.NextProtos = []string{"h3"}
				cache := &observedSessionCache{ClientSessionCache: tls.NewLRUClientSessionCache(4), stored: make(chan bool, 8)}
				ct.ClientSessionCache = cache
				socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				flight := &httpEarlyFlightGate{PacketConn: socket}
				arrival := &earlyArrivalObserver{PacketConn: flight, seen: make(chan struct{})}
				tr := &upstream.Transport{Conn: arrival}
				t.Cleanup(func() { socket.Close(); tr.Close() })
				address := socket.LocalAddr().String()
				origin := "https://" + address
				quicConfig := &quic.Config{Versions: []quic.Version{version}}
				for round := 0; round < 2; round++ {
					flight.held.Store(round == 1)
					listener, err := tr.ListenEarly(st, &upstream.Config{Allow0RTT: true, Versions: []upstream.Version{upstream.Version(version)}})
					if err != nil {
						t.Fatal(err)
					}
					requests := make(chan string, 8)
					server := &peerh3.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						requests <- r.URL.Path
						io.WriteString(w, "after handshake")
					})}
					done := make(chan struct{})
					go func() { defer close(done); server.ServeListener(listener) }()
					finish := func() { flight.held.Store(false); server.Close(); listener.Close(); <-done }

					var client *ownh3.ClientConn
					var loop sync.WaitGroup
					if mode == "DialEarly" {
						if client, err = ownh3.DialEarly(ctx, origin, ct, quicConfig, nil); err != nil {
							t.Fatal(err)
						}
					} else {
						conn, err := quic.DialAddrEarly(ctx, address, ct, quicConfig)
						if err != nil {
							t.Fatal(err)
						}
						if round == 1 {
							select {
							case <-conn.HandshakeComplete():
								t.Fatal("fixture did not return an early connection")
							default:
							}
						}
						if mode == "NewClientConn" {
							client = (&ownh3.Transport{}).NewClientConn(conn)
						} else {
							raw := (&ownh3.Transport{}).NewRawClientConn(conn)
							client = raw.ClientConn
							loop.Add(1)
							go func() {
								defer loop.Done()
								for {
									stream, err := conn.AcceptUniStream(ctx)
									if err != nil {
										return
									}
									go raw.HandleUnidirectionalStream(stream)
								}
							}()
						}
					}
					if round == 1 {
						// Bounded waits, as for the ordinary method in
						// TestHTTP3PublicEarlyRecovery: each must end with its context.
						waitCtx, stopWait := context.WithTimeout(ctx, 20*time.Millisecond)
						stream, waitErr := client.OpenRequestStream(waitCtx)
						stopWait()
						if stream != nil || !errors.Is(waitErr, context.DeadlineExceeded) {
							t.Fatal("raw request stream opened before the handshake:", waitErr)
						}
						if mode != "DialEarly" {
							// These connections have no remembered SETTINGS, so even the
							// explicit early method waits, and nothing may be on the wire.
							waitCtx, stopWait = context.WithTimeout(ctx, 20*time.Millisecond)
							early, _ := http.NewRequestWithContext(waitCtx, ownh3.MethodGet0RTT, origin+"/early", nil)
							_, waitErr = client.RoundTrip(early)
							stopWait()
							if !errors.Is(waitErr, context.DeadlineExceeded) {
								t.Fatal("request left before HTTP/3 setup:", waitErr)
							}
							select {
							case <-arrival.seen:
								t.Fatal("0-RTT packet from a connection that must not use early data")
							default:
							}
						}
						select {
						case path := <-requests:
							t.Fatal("request reached the server before the handshake:", path)
						default:
						}
						if flight.dropped.Load() == 0 {
							t.Fatal("server flight gate did not discard output")
						}
						flight.held.Store(false)
					}
					stream, err := client.OpenRequestStream(ctx)
					if err != nil {
						t.Fatal(err)
					}
					request, _ := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/raw", nil)
					if err := stream.SendRequestHeader(request); err != nil {
						t.Fatal(err)
					}
					if err := stream.Close(); err != nil {
						t.Fatal(err)
					}
					response, err := stream.ReadResponse()
					if err != nil || response.StatusCode != http.StatusOK {
						t.Fatal(response, err)
					}
					if body, err := io.ReadAll(stream); err != nil || string(body) != "after handshake" {
						t.Fatalf("%q %v", body, err)
					}
					if path := <-requests; path != "/raw" {
						t.Fatal("unexpected request", path)
					}
					if round == 0 {
						waitEarlyTicket(t, ctx, cache)
					}
					client.Close()
					loop.Wait()
					finish()
				}
			})
		}
	}
}
