package interop

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

func TestHTTP3ServerHandlerResetErrors(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, tunnelMode := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/tunnel=%t", version, tunnelMode), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				ct.NextProtos = []string{"h3"}
				st.NextProtos = []string{"h3"}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				listener, err := quic.ListenAddr("127.0.0.1:0", st, &quic.Config{Versions: []quic.Version{version}})
				if err != nil {
					t.Fatal(err)
				}
				entered := make(chan struct{})
				observed := make(chan []error, 1)
				server := &ownh3.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if tunnelMode {
						tunnel, err := ownh3.AcceptTunnel(w)
						if err != nil {
							observed <- []error{err}
							close(entered)
							return
						}
						close(entered)
						_, readErr := io.Copy(io.Discard, tunnel)
						_, writeErr := tunnel.Write([]byte("response"))
						closeErr := tunnel.CloseWrite()
						observed <- []error{readErr, writeErr, closeErr}
						tunnel.Close()
						return
					}
					close(entered)
					_, readErr := io.Copy(io.Discard, r.Body)
					_, writeErr := w.Write([]byte("response"))
					flushErr := http.NewResponseController(w).Flush()
					observed <- []error{readErr, writeErr, flushErr}
				})}
				served := make(chan struct{})
				go func() { defer close(served); server.Serve(ctx, listener) }()
				defer func() { server.Close(); listener.Close(); <-served }()
				client := &peerh3.Transport{TLSClientConfig: ct, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}}
				defer client.Close()
				input, output := io.Pipe()
				defer input.Close()
				defer output.Close()
				reqCtx, stop := context.WithCancel(ctx)
				defer stop()
				req, _ := http.NewRequestWithContext(reqCtx, "POST", "https://"+listener.Addr().String(), input)
				if tunnelMode {
					req.Method = "CONNECT"
					req.Host = "target.example:443"
				}
				completed := make(chan error, 1)
				go func() {
					response, err := client.RoundTrip(req)
					if response != nil {
						<-reqCtx.Done()
						response.Body.Close()
					}
					completed <- err
				}()
				defer func() {
					stop()
					client.Close()
					select {
					case <-completed:
					case <-time.After(time.Second):
						t.Error("client did not join")
					}
				}()
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				stop()
				select {
				case results := <-observed:
					if len(results) != 3 {
						t.Fatal("handler setup failed", results)
					}
					for i, err := range results {
						var h *ownh3.Error
						var stream *quic.StreamError
						if !errors.As(err, &h) || !errors.As(err, &stream) || uint64(h.ErrorCode) != uint64(stream.ErrorCode) || h.Remote != stream.Remote {
							t.Fatalf("operation %d: %v", i, err)
						}
					}
				case <-ctx.Done():
					t.Fatal("handler blocked", ctx.Err())
				}
			})
		}
	}
}
