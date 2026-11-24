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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

func TestHTTP3TransportEarlyRecovery(t *testing.T) {
	testHTTP3TransportEarlyRecovery(t, false)
}

func TestHTTP3CustomDialEarlyRecovery(t *testing.T) {
	testHTTP3TransportEarlyRecovery(t, true)
}

func testHTTP3TransportEarlyRecovery(t *testing.T, custom bool) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, accept := range []bool{true, false} {
			for _, mode := range []struct {
				name              string
				retry, closeEarly bool
			}{
				{"retry", true, false}, {"cached-retry", true, false}, {"cached-no-retry", false, false}, {"no-retry", false, false}, {"close-handshake", true, true},
			} {
				retry := mode.retry
				t.Run(fmt.Sprintf("%x/accept=%t/%s", version, accept, mode.name), func(t *testing.T) {
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
					tr := &upstream.Transport{Conn: arrival}
					t.Cleanup(func() { socket.Close(); tr.Close() })
					origin := "https://" + socket.LocalAddr().String()
					callbackErr := fmt.Errorf("application callback: %w", quic.Err0RTTRejected)
					var callbacks atomic.Int32
					pool := &ownh3.Transport{ClientConfig: &ownh3.ClientConfig{Information: func(status int, _ http.Header) error {
						if status == http.StatusEarlyHints {
							callbacks.Add(1)
							return callbackErr
						}
						return nil
					}}, Enable0RTT: true, DisableRetries: !retry, MaxConnections: 1, TLSClientConfig: ct, QUICConfig: &quic.Config{Versions: []quic.Version{version}}}
					defer pool.Close()
					var dials atomic.Int32
					if custom {
						pool.Dial = func(ctx context.Context, address string, tlsConfig *tls.Config, config *quic.Config) (*quic.Conn, error) {
							dials.Add(1)
							return quic.DialAddrEarly(ctx, address, tlsConfig, config)
						}
					}
					for round := 0; round < 2; round++ {
						listener, err := tr.ListenEarly(st, &upstream.Config{Allow0RTT: round == 0 || accept, Versions: []upstream.Version{upstream.Version(version)}})
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { listener.Close() })
						var requests atomic.Int32
						server := &peerh3.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							requests.Add(1)
							if r.URL.Path == "/spoof" {
								w.WriteHeader(http.StatusEarlyHints)
							}
							if r.Method == http.MethodHead && r.Header.Get("Accept-Encoding") != "" {
								http.Error(w, "gzip added to HEAD", 500)
								return
							}
							w.Write([]byte("pooled early HTTP"))
						})}
						done := make(chan struct{})
						go func() {
							defer close(done)
							peer, err := listener.Accept(ctx)
							if err == nil {
								server.ServeQUICConn(peer)
							}
						}()
						t.Cleanup(func() { server.Close(); listener.Close(); <-done })
						method := http.MethodGet
						if round == 1 {
							method = ownh3.MethodHead0RTT
						}
						request, _ := http.NewRequestWithContext(ctx, method, origin+"/", nil)
						// A rejection is only certain for a request the client began
						// while early data was still its only option. Its body reports
						// when the client has opened the stream and is sending it; the
						// handshake is held until then. Without that, a slow client can
						// open the stream after the handshake and send it as 1-RTT, which
						// is correct: a request that never went early cannot be rejected,
						// and with retries disabled it simply succeeds. A bodyless
						// HEAD0RTT gives no such signal, so rejection is checked with a
						// GET0RTT; it is handled the same way with or without a body.
						// It stays replayable, so a retry would happen if retries were
						// not disabled, and the count below would show it.
						var rejectedBody *cachedEarlyAdmissionBody
						var rejectedReplays atomic.Int32
						if round == 1 && !accept && !retry {
							rejectedBody = &cachedEarlyAdmissionBody{entered: make(chan struct{})}
							method = ownh3.MethodGet0RTT
							request, _ = http.NewRequestWithContext(ctx, method, origin+"/", rejectedBody)
							request.ContentLength = -1
							request.GetBody = func() (io.ReadCloser, error) {
								rejectedReplays.Add(1)
								return &countedHTTPBody{Reader: strings.NewReader("")}, nil
							}
						}
						type outcome struct {
							response *http.Response
							err      error
						}
						var cachedResult chan outcome
						var cachedBody *cachedEarlyAdmissionBody
						var cachedRecreated *countedHTTPBody
						var cachedReplays atomic.Int32
						result := make(chan outcome, 1)
						go func() { response, err := pool.RoundTrip(request); result <- outcome{response, err} }()
						if round == 1 {
							select {
							case <-arrival.seen:
							case <-ctx.Done():
								t.Fatal("no pooled early wire packet", ctx.Err())
							}
							if rejectedBody != nil {
								select {
								case <-rejectedBody.entered:
								case <-ctx.Done():
									t.Fatal("early request never began sending", ctx.Err())
								}
							}
							if strings.HasPrefix(mode.name, "cached-") {
								cachedResult = make(chan outcome, 1)
								cachedBody = &cachedEarlyAdmissionBody{entered: make(chan struct{})}
								cachedRequest, _ := http.NewRequestWithContext(ctx, ownh3.MethodGet0RTT, origin+"/cached", cachedBody)
								cachedRequest.ContentLength = -1
								cachedRecreated = &countedHTTPBody{Reader: strings.NewReader("")}
								cachedRequest.GetBody = func() (io.ReadCloser, error) { cachedReplays.Add(1); return cachedRecreated, nil }
								go func() {
									response, err := pool.RoundTripOpt(cachedRequest, ownh3.RoundTripOpt{OnlyCachedConn: true})
									cachedResult <- outcome{response, err}
								}()
								select {
								case <-cachedBody.entered:
								case got := <-cachedResult:
									if got.response != nil {
										got.response.Body.Close()
									}
									t.Fatal("cached request did not join early slot", got.err)
								case <-ctx.Done():
									t.Fatal("cached upload not admitted before handshake", ctx.Err())
								}
								if requests.Load() != 0 {
									t.Fatal("request reached application behind TLS gate")
								}
							}
							if mode.closeEarly {
								closed := make(chan error, 1)
								go func() { closed <- pool.Close() }()
								select {
								case err := <-closed:
									if err != nil {
										t.Fatal(err)
									}
								case <-ctx.Done():
									t.Fatal("pool Close did not join early worker", ctx.Err())
								}
								select {
								case got := <-result:
									if got.response != nil {
										got.response.Body.Close()
										t.Fatal("closed early request returned response")
									}
									if got.err == nil {
										t.Fatal("closed early request succeeded")
									}
								case <-ctx.Done():
									t.Fatal("closed early request stuck", ctx.Err())
								}
								if requests.Load() != 0 {
									t.Fatal("request processed behind TLS gate")
								}
								late, _ := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/closed", nil)
								if _, err := pool.RoundTrip(late); !errors.Is(err, ownh3.ErrTransportClosed) {
									t.Fatal("closed pool admitted request", err)
								}
								release()
								return
							}
							release()
						}
						var got outcome
						select {
						case got = <-result:
						case <-ctx.Done():
							t.Fatal("pooled request stuck", ctx.Err())
						}
						if round == 1 && !accept && !retry {
							if !errors.Is(got.err, quic.Err0RTTRejected) {
								resumed := false
								if got.response != nil {
									if got.response.TLS != nil {
										resumed = got.response.TLS.DidResume
									}
									got.response.Body.Close()
								}
								t.Fatal("retries disabled", got.err, "; requests", requests.Load(), "hellos", hellos.Load(), "dials", dials.Load(), "resumed", resumed, ";", arrival.summary())
							}
							if requests.Load() != 0 {
								t.Fatal("rejected request processed")
							}
							if replays := rejectedReplays.Load(); replays != 0 {
								t.Fatal("rejected request replayed with retries disabled:", replays)
							}
							fresh, _ := http.NewRequestWithContext(ctx, http.MethodHead, origin+"/fresh", nil)
							got.response, got.err = pool.RoundTrip(fresh)
						}
						if got.err != nil {
							t.Fatal(got.err)
						}
						body, err := io.ReadAll(got.response.Body)
						got.response.Body.Close()
						expected := "pooled early HTTP"
						if round == 1 {
							expected = ""
						}
						if err != nil || got.response.StatusCode != 200 || string(body) != expected {
							t.Fatal(got.response.StatusCode, string(body), err)
						}
						if request.Method != method {
							t.Fatal("caller method mutated")
						}
						expectedRequests := int32(1)
						if cachedResult != nil {
							var cached outcome
							select {
							case cached = <-cachedResult:
							case <-ctx.Done():
								t.Fatal("cached recovery stalled", ctx.Err())
							}
							if !accept && !retry {
								if cached.response != nil || !errors.Is(cached.err, quic.Err0RTTRejected) {
									t.Fatal("cached rejection was replayed", cached.response, cached.err)
								}
								fresh, _ := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/cached-fresh", nil)
								cached.response, cached.err = pool.RoundTripOpt(fresh, ownh3.RoundTripOpt{OnlyCachedConn: true})
							}
							if cached.err != nil {
								t.Fatal("cached recovery failed", cached.err)
							}
							payload, err := io.ReadAll(cached.response.Body)
							cached.response.Body.Close()
							wantReplays := int32(0)
							if !accept && retry {
								wantReplays = 1
							}
							if err != nil || string(payload) != "pooled early HTTP" || cached.response.StatusCode != 200 || cached.response.TLS == nil || !cached.response.TLS.DidResume || cachedReplays.Load() != wantReplays || cachedBody.closes.Load() != 1 || cachedRecreated.closes.Load() != wantReplays {
								t.Fatal("cached recovery outcome", string(payload), err, cachedReplays.Load(), cachedBody.closes.Load(), cachedRecreated.closes.Load())
							}
							expectedRequests++
						}
						if requests.Load() != expectedRequests {
							t.Fatal("unexpected application requests", requests.Load(), expectedRequests)
						}
						if got.response.TLS == nil || got.response.TLS.DidResume != (round == 1) {
							t.Fatal("resumption lost")
						}
						if round == 0 {
							waitEarlyTicket(t, ctx, cache)
						}
						if round == 1 {
							failures := make(chan error, 8)
							for i := 0; i < cap(failures); i++ {
								go func() {
									req, _ := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/concurrent", nil)
									res, err := pool.RoundTrip(req)
									if err == nil {
										body, readErr := io.ReadAll(res.Body)
										res.Body.Close()
										err = readErr
										if err == nil && string(body) != "pooled early HTTP" {
											err = fmt.Errorf("wrong concurrent body")
										}
									}
									failures <- err
								}()
							}
							for i := 0; i < cap(failures); i++ {
								if err := <-failures; err != nil {
									t.Fatal("concurrent pooled request", err)
								}
							}
							if requests.Load() != expectedRequests+8 {
								t.Fatal("concurrent request count", requests.Load())
							}
							spoof, _ := http.NewRequestWithContext(ctx, ownh3.MethodGet0RTT, origin+"/spoof", nil)
							res, err := pool.RoundTrip(spoof)
							if res != nil {
								res.Body.Close()
								t.Fatal("callback failure returned response")
							}
							if !errors.Is(err, callbackErr) {
								t.Fatal("callback cause lost", err)
							}
							if callbacks.Load() != 1 || requests.Load() != expectedRequests+9 {
								t.Fatal("callback error caused replay", callbacks.Load(), requests.Load())
							}
							fresh, _ := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/after-spoof", nil)
							res, err = pool.RoundTrip(fresh)
							if err != nil {
								t.Fatal("callback error poisoned pool", err)
							}
							_, err = io.Copy(io.Discard, res.Body)
							res.Body.Close()
							if err != nil || requests.Load() != expectedRequests+10 {
								t.Fatal("request after callback failure", err, requests.Load())
							}

						}
						pool.CloseIdleConnections()
						server.Close()
						listener.Close()
						<-done
					}
					if hellos.Load() != 2 {
						t.Fatal("pool redialed during recovery", hellos.Load())
					}
					if custom && dials.Load() != 2 {
						t.Fatal("custom dialer not used for both connections", dials.Load())
					}
				})
			}
		}
	}
}

// Read is reached only after the cached request has acquired an HTTP stream and
// admitted its headers. The fixture keeps server TLS gated until this signal.
type cachedEarlyAdmissionBody struct {
	entered chan struct{}
	once    sync.Once
	closes  atomic.Int32
}

func (b *cachedEarlyAdmissionBody) Read([]byte) (int, error) {
	b.once.Do(func() { close(b.entered) })
	return 0, io.EOF
}
func (b *cachedEarlyAdmissionBody) Close() error { b.closes.Add(1); return nil }
