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
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

func TestHTTP3ServerConnectionContext(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, mode := range []string{"default", "custom", "detached"} {
			t.Run(fmt.Sprintf("%x/%s", v, mode), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				type parentKey struct{}
				type customKey struct{}
				serverCtx, stopServer := context.WithCancelCause(context.WithValue(ctx, parentKey{}, 42))
				defer stopServer(context.Canceled)
				ct, st := tlsConfigs(t)
				ct.NextProtos = []string{"h3"}
				st.NextProtos = []string{"h3"}
				listener, err := quic.ListenAddr("127.0.0.1:0", st, &quic.Config{Versions: []quic.Version{v}})
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				var callbacks atomic.Int32
				hookCancel := make(chan context.CancelCauseFunc, 1)
				hookErrors := make(chan error, 1)
				checks := make(chan error, 2)
				entered := make(chan struct{}, 1)
				canceled := make(chan error, 1)
				server := &ownh3.Server{}
				inspect := func(c context.Context) error {
					if c.Value(ownh3.ServerContextKey) != server {
						return fmt.Errorf("server identity missing")
					}
					local, ok := c.Value(http.LocalAddrContextKey).(net.Addr)
					if !ok || local.String() != listener.Addr().String() {
						return fmt.Errorf("local address missing: %v", local)
					}
					if _, ok := c.Value(ownh3.RemoteAddrContextKey).(net.Addr); !ok {
						return fmt.Errorf("remote address missing")
					}
					if c.Value(parentKey{}) != 42 {
						return fmt.Errorf("parent value missing")
					}
					return nil
				}
				if mode != "default" {
					server.ConnContext = func(base context.Context, conn *quic.Conn) context.Context {
						callbacks.Add(1)
						err := inspect(base)
						if err == nil && base.Value(ownh3.RemoteAddrContextKey).(net.Addr).String() != conn.RemoteAddr().String() {
							err = fmt.Errorf("callback address mismatch")
						}
						hookErrors <- err
						if mode == "detached" {
							base = context.Background()
						}
						selected, stop := context.WithCancelCause(context.WithValue(base, customKey{}, "connection-value"))
						hookCancel <- stop
						return selected
					}
				}
				server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var err error
					if mode != "detached" {
						err = inspect(r.Context())
						if err == nil && r.Context().Value(ownh3.RemoteAddrContextKey).(net.Addr).String() != r.RemoteAddr {
							err = fmt.Errorf("request remote mismatch")
						}
					}
					if mode != "default" && r.Context().Value(customKey{}) != "connection-value" {
						err = fmt.Errorf("custom value missing")
					}
					checks <- err
					if r.URL.Path == "/blocked" {
						w.(http.Flusher).Flush()
						entered <- struct{}{}
						<-r.Context().Done()
						canceled <- context.Cause(r.Context())
						return
					}
					io.WriteString(w, "context visible")
				})
				served := make(chan error, 1)
				go func() { served <- server.Serve(serverCtx, listener) }()
				defer func() { server.Close(); listener.Close(); <-served }()
				peer, err := dialUpstreamAddr(t, ctx, listener.Addr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}})
				if err != nil {
					t.Fatal(err)
				}
				defer peer.CloseWithError(0, "done")
				transport := &peerh3.Transport{}
				defer transport.Close()
				client := transport.NewClientConn(peer)
				origin := "https://" + listener.Addr().String()
				request, _ := http.NewRequestWithContext(ctx, "GET", origin+"/first", nil)
				response, err := client.RoundTrip(request)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil || string(body) != "context visible" {
					t.Fatal(string(body), err)
				}
				request, _ = http.NewRequestWithContext(ctx, "GET", origin+"/blocked", nil)
				response, err = client.RoundTrip(request)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				for i := 0; i < 2; i++ {
					if err := <-checks; err != nil {
						t.Fatal(err)
					}
				}
				cause := errors.New("context stopped")
				want := error(cause)
				switch mode {
				case "default":
					stopServer(cause)
				case "custom":
					stop := <-hookCancel
					defer stop(context.Canceled)
					stop(cause)
				case "detached":
					stop := <-hookCancel
					defer stop(context.Canceled)
					want = ownh3.ErrServerClosed
					server.Close()
				}
				select {
				case err := <-canceled:
					if (mode != "detached" && !errors.Is(err, want)) || (mode == "detached" && (err == nil || errors.Is(err, context.DeadlineExceeded))) {
						t.Fatal("handler cause", err, want)
					}
				case <-ctx.Done():
					t.Fatal("handler detached from lifetime", ctx.Err())
				}
				expected := int32(0)
				if mode != "default" {
					expected = 1
					if err := <-hookErrors; err != nil {
						t.Fatal(err)
					}
				}
				if callbacks.Load() != expected {
					t.Fatal("callback count", callbacks.Load())
				}
			})
		}
	}
}

func TestHTTP3NilConnectionContextCleanup(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", v), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ct, st := tlsConfigs(t)
			ct.NextProtos = []string{"h3"}
			st.NextProtos = []string{"h3"}
			listener, err := quic.ListenAddr("127.0.0.1:0", st, &quic.Config{Versions: []quic.Version{v}})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			peer, err := dialUpstreamAddr(t, ctx, listener.Addr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}})
			if err != nil {
				t.Fatal(err)
			}
			defer peer.CloseWithError(0, "done")
			conn, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			server := &ownh3.Server{ConnContext: func(context.Context, *quic.Conn) context.Context { return nil }}
			defer server.Close()
			var caught any
			func() { defer func() { caught = recover() }(); server.ServeQUICConn(ctx, conn) }()
			if caught != "http3: ConnContext returned nil" {
				t.Fatal("nil callback outcome", caught)
			}
			if err := server.Shutdown(ctx); err != nil {
				t.Fatal("panic leaked connection admission", err)
			}
			select {
			case <-conn.Context().Done():
			case <-ctx.Done():
				t.Fatal("panic left connection open", ctx.Err())
			}
		})
	}
}
