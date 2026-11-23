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

func TestHTTP3TransportTooEarly(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, mode := range []string{"retry", "repeat", "disabled", "forwarded", "ordinary", "body", "unrepeatable", "trailers", "head", "upload-stop", "getbody-error", "cancel-getbody", "close-getbody", "nil-getbody"} {
			t.Run(fmt.Sprintf("%x/%s", version, mode), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				ct, st := tlsConfigs(t)
				cache := &observedSessionCache{ClientSessionCache: tls.NewLRUClientSessionCache(4), stored: make(chan bool, 8)}
				ct.ClientSessionCache = cache
				type connectionKey struct{}
				var attempts, replays atomic.Int32
				var beforeTLS atomic.Bool
				var bodyBad atomic.Bool
				server := &peerh3.Server{TLSConfig: st, QUICConfig: &upstream.Config{Allow0RTT: true, Versions: []upstream.Version{upstream.Version(version)}}, ConnContext: func(ctx context.Context, conn *upstream.Conn) context.Context {
					return context.WithValue(ctx, connectionKey{}, conn)
				}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/seed" {
						w.Write([]byte("seed"))
						return
					}
					attempt := attempts.Add(1)
					var body []byte
					var err error
					if mode != "upload-stop" || attempt > 1 {
						body, err = io.ReadAll(r.Body)
					}
					if err != nil || ((mode == "body" || mode == "upload-stop" && attempt > 1) && string(body) != "replayable payload") {
						bodyBad.Store(true)
					}
					if attempt > 1 {
						conn := r.Context().Value(connectionKey{}).(*upstream.Conn)
						select {
						case <-conn.HandshakeComplete():
						default:
							beforeTLS.Store(true)
						}
					}
					if attempt == 1 || mode == "repeat" {
						w.WriteHeader(http.StatusTooEarly)
						w.Write([]byte("too early body"))
						return
					}
					w.Write([]byte("retry accepted"))
				})}
				socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				done := make(chan struct{})
				go func() { defer close(done); server.Serve(socket) }()
				defer func() { server.Close(); socket.Close(); <-done }()
				pool := &ownh3.Transport{ClientConfig: &ownh3.ClientConfig{PermitsTrailer: func(name string) bool { return name == "x-test" }}, Enable0RTT: true, DisableRetries: mode == "disabled", MaxConnections: 1, TLSClientConfig: ct, QUICConfig: &quic.Config{Versions: []quic.Version{version}}}
				defer pool.Close()
				origin := "https://" + socket.LocalAddr().String()
				seed, _ := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/seed", nil)
				response, err := pool.RoundTrip(seed)
				if err != nil {
					t.Fatal(err)
				}
				io.Copy(io.Discard, response.Body)
				response.Body.Close()
				waitEarlyTicket(t, ctx, cache)
				pool.CloseIdleConnections()
				method := ownh3.MethodGet0RTT
				if mode == "head" {
					method = ownh3.MethodHead0RTT
				}
				if mode == "ordinary" {
					method = http.MethodGet
				}
				request, _ := http.NewRequestWithContext(ctx, method, origin+"/request", nil)
				if mode == "forwarded" {
					request.Header["eArLy-DaTa"] = []string{"1"}
				}
				if mode == "body" || mode == "unrepeatable" {
					request.Body = io.NopCloser(strings.NewReader("replayable payload"))
					request.ContentLength = int64(len("replayable payload"))
					if mode == "body" {
						request.GetBody = func() (io.ReadCloser, error) {
							replays.Add(1)
							return io.NopCloser(strings.NewReader("replayable payload")), nil
						}
					}
				}
				if mode == "upload-stop" {
					upload := &tooEarlyBlockedBody{done: make(chan struct{})}
					request.Body, request.ContentLength = upload, -1
					request.GetBody = func() (io.ReadCloser, error) {
						select {
						case <-upload.done:
						default:
							return nil, fmt.Errorf("old upload still open during GetBody")
						}
						replays.Add(1)
						return io.NopCloser(strings.NewReader("replayable payload")), nil
					}
				}
				if mode == "trailers" {
					request.Trailer = http.Header{"X-Test": []string{"value"}}
				}
				failureMode := mode == "getbody-error" || mode == "cancel-getbody" || mode == "close-getbody" || mode == "nil-getbody"
				cause := errors.New("425 body recreation failure")
				var originalBody, recreatedBody *countedHTTPBody
				if failureMode {
					requestCtx, cancelRequest := context.WithCancelCause(ctx)
					defer cancelRequest(context.Canceled)
					request = request.WithContext(requestCtx)
					originalBody = &countedHTTPBody{Reader: strings.NewReader("replayable payload")}
					recreatedBody = &countedHTTPBody{Reader: strings.NewReader("replayable payload")}
					request.Body, request.ContentLength = originalBody, int64(len("replayable payload"))
					request.GetBody = func() (io.ReadCloser, error) {
						replays.Add(1)
						if originalBody.closes.Load() != 1 {
							return nil, fmt.Errorf("old upload not closed exactly once")
						}
						switch mode {
						case "getbody-error":
							return recreatedBody, cause
						case "cancel-getbody":
							cancelRequest(cause)
						case "close-getbody":
							pool.Close()
						case "nil-getbody":
							return nil, nil
						}
						return recreatedBody, nil
					}
				}
				response, err = pool.RoundTrip(request)
				if failureMode {
					if response != nil {
						response.Body.Close()
						t.Fatal("recreation failure returned response")
					}
					expected := cause
					if mode == "close-getbody" {
						expected = ownh3.ErrTransportClosed
					}
					if err == nil || (mode != "nil-getbody" && !errors.Is(err, expected)) {
						t.Fatal("recreation failure cause", err)
					}
					wantCloses := int32(1)
					if mode == "nil-getbody" {
						wantCloses = 0
					}
					if originalBody.closes.Load() != 1 || recreatedBody.closes.Load() != wantCloses || replays.Load() != 1 || attempts.Load() != 1 {
						t.Fatal("recreation ownership/attempt count", originalBody.closes.Load(), recreatedBody.closes.Load(), replays.Load(), attempts.Load())
					}
					if request.Method != method {
						t.Fatal("caller method changed")
					}
					fresh, _ := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/after", nil)
					response, err = pool.RoundTrip(fresh)
					if mode == "close-getbody" {
						if !errors.Is(err, ownh3.ErrTransportClosed) || attempts.Load() != 1 {
							t.Fatal("closed pool reused", err)
						}
					} else {
						if err != nil {
							t.Fatal("recreation failure leaked pool reservation", err)
						}
						_, err = io.Copy(io.Discard, response.Body)
						response.Body.Close()
						if err != nil || response.StatusCode != 200 || attempts.Load() != 2 {
							t.Fatal("fresh request after recreation failure", err, response.StatusCode, attempts.Load())
						}
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				wantAttempts, wantStatus, wantBody := int32(1), http.StatusTooEarly, "too early body"
				if mode == "retry" || mode == "body" || mode == "head" || mode == "upload-stop" {
					wantAttempts, wantStatus, wantBody = 2, 200, "retry accepted"
				}
				if mode == "repeat" {
					wantAttempts = 2
				}
				if mode == "head" {
					wantBody = ""
				}
				if err != nil || response.StatusCode != wantStatus || string(body) != wantBody || attempts.Load() != wantAttempts {
					t.Fatal(response.StatusCode, string(body), err, attempts.Load())
				}
				if response.Request != request || request.Method != method {
					t.Fatal("request ownership changed")
				}
				if beforeTLS.Load() || bodyBad.Load() {
					t.Fatal("retry sent early or body changed", beforeTLS.Load(), bodyBad.Load())
				}
				if ((mode == "body" || mode == "upload-stop") && replays.Load() != 1) || (mode != "body" && mode != "upload-stop" && replays.Load() != 0) {
					t.Fatal("body recreation count", replays.Load())
				}
			})
		}
	}
}

// Close interrupts a blocked uploader, as required for concurrent RoundTrip.
type tooEarlyBlockedBody struct {
	done chan struct{}
	once sync.Once
}

func (b *tooEarlyBlockedBody) Read([]byte) (int, error) { <-b.done; return 0, io.ErrClosedPipe }
func (b *tooEarlyBlockedBody) Close() error             { b.once.Do(func() { close(b.done) }); return nil }
