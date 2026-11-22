package interop

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	"github.com/getlantern/photon/internal/http3conn"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

func TestHTTP3EarlyDataResponseMisuse(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, mode := range []string{"response", "informational", "declaration", "trailer"} {
			t.Run(fmt.Sprintf("%x/%s", version, mode), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				ct, st := tlsConfigs(t)
				var handled, connections, information atomic.Int32
				server := &peerh3.Server{TLSConfig: st, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}, ConnContext: func(ctx context.Context, _ *upstream.Conn) context.Context { connections.Add(1); return ctx }, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					handled.Add(1)
					if r.URL.Path == "/healthy" {
						w.Write([]byte("healthy connection"))
						return
					}
					switch mode {
					case "response":
						w.Header().Set("Early-Data", "1")
					case "informational":
						w.Header().Set("Early-Data", "1")
						w.WriteHeader(http.StatusEarlyHints)
						w.Header().Del("Early-Data")
					case "declaration":
						w.Header().Set("Trailer", "Early-Data")
					case "trailer":
						w.WriteHeader(http.StatusOK)
						w.(http.Flusher).Flush()
						w.Header().Set(http.TrailerPrefix+"Early-Data", "1")
						return
					}
					w.WriteHeader(http.StatusOK)
				})}
				socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				done := make(chan struct{})
				go func() { defer close(done); server.Serve(socket) }()
				defer func() { server.Close(); socket.Close(); <-done }()
				origin := "https://" + socket.LocalAddr().String()
				client, err := ownh3.Dial(ctx, origin, ct, &quic.Config{Versions: []quic.Version{version}}, &ownh3.ClientConfig{MaxConcurrentRequests: 1, Information: func(int, http.Header) error { information.Add(1); return nil }})
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				request, _ := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/invalid", nil)
				response, err := client.RoundTrip(request)
				if mode == "trailer" {
					if err != nil || response == nil {
						t.Fatal("valid initial response rejected", err)
					}
					_, err = io.ReadAll(response.Body)
				} else if response != nil {
					response.Body.Close()
					t.Fatal("invalid initial fields returned response")
				}
				if response != nil {
					response.Body.Close()
					if len(response.Trailer.Values("Early-Data")) != 0 {
						t.Fatal("invalid trailer exposed to caller")
					}
				}
				var malformed *http3conn.MessageError
				if !errors.As(err, &malformed) {
					t.Fatal("missing message error", err)
				}
				if information.Load() != 0 {
					t.Fatal("invalid informational fields reached callback")
				}
				request, _ = http.NewRequestWithContext(ctx, http.MethodGet, origin+"/healthy", nil)
				response, err = client.RoundTrip(request)
				if err != nil {
					t.Fatal("field error broke connection or leaked request slot", err)
				}
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil || string(body) != "healthy connection" || connections.Load() != 1 || handled.Load() != 2 {
					t.Fatal(string(body), err, connections.Load(), handled.Load())
				}
			})
		}
	}
}
