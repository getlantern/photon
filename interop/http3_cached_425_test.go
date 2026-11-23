package interop

import (
	"context"
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

// Exercise the explicit replayable-method 425 policy on an already-warm
// connection. This checks retry admission, not pre-handshake packet delivery.
func TestHTTP3CachedOnly425(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, evict := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/evict=%v", version, evict), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				ct, st := tlsConfigs(t)
				var connections, attempts atomic.Int32
				server := &peerh3.Server{TLSConfig: st, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}, ConnContext: func(ctx context.Context, _ *upstream.Conn) context.Context { connections.Add(1); return ctx }, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/warm" {
						io.WriteString(w, "warm")
						return
					}
					attempt := attempts.Add(1)
					body, err := io.ReadAll(r.Body)
					if err != nil || string(body) != "payload" {
						http.Error(w, "bad upload", 500)
						return
					}
					if attempt == 1 {
						w.WriteHeader(http.StatusTooEarly)
						io.WriteString(w, "retry later")
						return
					}
					io.WriteString(w, "accepted")
				})}
				socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				done := make(chan struct{})
				go func() { defer close(done); server.Serve(socket) }()
				defer func() { server.Close(); socket.Close(); <-done }()
				pool := &ownh3.Transport{Enable0RTT: true, MaxConnections: 1, TLSClientConfig: ct, QUICConfig: &quic.Config{Versions: []quic.Version{version}}}
				defer pool.Close()
				origin := "https://" + socket.LocalAddr().String()
				warm, _ := http.NewRequestWithContext(ctx, "GET", origin+"/warm", nil)
				response, err := pool.RoundTrip(warm)
				if err != nil {
					t.Fatal(err)
				}
				_, err = io.Copy(io.Discard, response.Body)
				response.Body.Close()
				if err != nil {
					t.Fatal(err)
				}
				original := &countedHTTPBody{Reader: strings.NewReader("payload")}
				recreated := &countedHTTPBody{Reader: strings.NewReader("payload")}
				request, _ := http.NewRequestWithContext(ctx, ownh3.MethodGet0RTT, origin+"/request", original)
				request.ContentLength = 7
				replays := 0
				request.GetBody = func() (io.ReadCloser, error) {
					replays++
					if original.closes.Load() != 1 {
						return nil, fmt.Errorf("upload not closed before replay")
					}
					if evict {
						pool.CloseIdleConnections()
					}
					return recreated, nil
				}
				response, err = pool.RoundTripOpt(request, ownh3.RoundTripOpt{OnlyCachedConn: true})
				if evict {
					if response != nil || !errors.Is(err, ownh3.ErrNoCachedConn) || attempts.Load() != 1 {
						t.Fatal("retry redialed or lost error", response, err, attempts.Load())
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					body, readErr := io.ReadAll(response.Body)
					response.Body.Close()
					if readErr != nil || response.StatusCode != 200 || string(body) != "accepted" || attempts.Load() != 2 {
						t.Fatal("cached retry failed", response.StatusCode, string(body), readErr, attempts.Load())
					}
					if response.Request != request {
						t.Fatal("request identity lost")
					}
				}
				if connections.Load() != 1 || replays != 1 || original.closes.Load() != 1 || recreated.closes.Load() != 1 || request.Method != ownh3.MethodGet0RTT {
					t.Fatal("retry ownership", connections.Load(), replays, original.closes.Load(), recreated.closes.Load(), request.Method)
				}
			})
		}
	}
}
