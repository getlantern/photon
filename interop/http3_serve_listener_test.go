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

func TestHTTP3ServeListenerOwnership(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, early := range []bool{false, true} {
			for _, graceful := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/early=%t/graceful=%t", version, early, graceful), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					ct, st := tlsConfigs(t)
					ct.NextProtos, st.NextProtos = []string{"h3"}, []string{"h3"}
					var listener ownh3.QUICListener
					var err error
					cfg := &quic.Config{Versions: []quic.Version{version}}
					if early {
						listener, err = quic.ListenAddrEarly("127.0.0.1:0", st, cfg)
					} else {
						listener, err = quic.ListenAddr("127.0.0.1:0", st, cfg)
					}
					if err != nil {
						t.Fatal(err)
					}
					defer listener.Close()
					// A fresh HTTP server must be able to reuse the same live QUIC listener.
					for round := 0; round < 2; round++ {
						func() {
							release := make(chan struct{})
							handlerCtx := make(chan context.Context, 1)
							server := &ownh3.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
								io.WriteString(w, "before")
								w.(http.Flusher).Flush()
								handlerCtx <- r.Context()
								select {
								case <-release:
									io.WriteString(w, "after")
								case <-r.Context().Done():
								}
							})}
							done := make(chan struct{})
							var serveErr error
							go func() { defer close(done); serveErr = server.ServeListener(listener) }()
							defer func() {
								server.Close()
								cleanup, stop := context.WithTimeout(context.Background(), time.Second)
								defer stop()
								if err := server.Shutdown(cleanup); err != nil {
									t.Error(err)
								}
								select {
								case <-done:
								case <-time.After(time.Second):
									t.Error("listener worker did not join")
								}
							}()
							peer, err := dialUpstreamAddr(t, ctx, listener.Addr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}})
							if err != nil {
								t.Fatal("listener could not be reused", err)
							}
							defer peer.CloseWithError(0, "")
							transport := &peerh3.Transport{}
							defer transport.Close()
							client := transport.NewClientConn(peer)
							req, _ := http.NewRequestWithContext(ctx, "GET", "https://"+listener.Addr().String()+"/", nil)
							response, err := client.RoundTrip(req)
							if err != nil {
								t.Fatal(err)
							}
							defer response.Body.Close()
							prefix := make([]byte, 6)
							if _, err := io.ReadFull(response.Body, prefix); err != nil || string(prefix) != "before" {
								t.Fatalf("response prefix %q: %v", prefix, err)
							}
							var requestCtx context.Context
							select {
							case requestCtx = <-handlerCtx:
							case <-ctx.Done():
								t.Fatal(ctx.Err())
							}
							if graceful {
								deadline, stop := context.WithTimeout(ctx, 20*time.Millisecond)
								err := server.Shutdown(deadline)
								stop()
								if !errors.Is(err, context.DeadlineExceeded) {
									t.Fatal("active handler not retained", err)
								}
								if requestCtx.Err() != nil {
									t.Fatal("graceful shutdown canceled handler", requestCtx.Err())
								}
								select {
								case <-done:
									if !errors.Is(serveErr, http.ErrServerClosed) {
										t.Fatal(serveErr)
									}
								case <-ctx.Done():
									t.Fatal("ServeListener waited for active handler after shutdown")
								}
								close(release)
								body, err := io.ReadAll(response.Body)
								if err != nil || string(body) != "after" {
									t.Fatalf("drained response %q: %v", body, err)
								}
								response.Body.Close()
								if err := server.Shutdown(ctx); err != nil {
									t.Fatal(err)
								}
							} else {
								if err := server.Close(); err != nil {
									t.Fatal(err)
								}
								select {
								case <-requestCtx.Done():
								case <-ctx.Done():
									t.Fatal("Close did not cancel handler")
								}
							}
							select {
							case <-done:
								if !errors.Is(serveErr, http.ErrServerClosed) {
									t.Fatal("ServeListener result", serveErr)
								}
							case <-ctx.Done():
								t.Fatal("ServeListener did not stop", ctx.Err())
							}
						}()
					}
				})
			}
		}
	}
}

// Return an application error after accepting one connection, on demand.
type failingHTTPListener struct {
	ownh3.QUICListener
	fail     <-chan struct{}
	err      error
	accepted bool
}

func (l *failingHTTPListener) Accept(ctx context.Context) (*quic.Conn, error) {
	if !l.accepted {
		l.accepted = true
		return l.QUICListener.Accept(ctx)
	}
	select {
	case <-l.fail:
		return nil, l.err
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

func TestHTTP3ServeListenerAcceptFailureRetainsRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ct, st := tlsConfigs(t)
	ct.NextProtos, st.NextProtos = []string{"h3"}, []string{"h3"}
	listener, err := quic.ListenAddr("127.0.0.1:0", st, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	fail := make(chan struct{})
	want := errors.New("application stopped accepting")
	wrapper := &failingHTTPListener{QUICListener: listener, fail: fail, err: want}
	release := make(chan struct{})
	entered := make(chan context.Context, 1)
	server := &ownh3.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "before")
		w.(http.Flusher).Flush()
		entered <- r.Context()
		select {
		case <-release:
			io.WriteString(w, "after")
		case <-r.Context().Done():
		}
	})}
	done := make(chan error, 1)
	go func() { done <- server.ServeListener(wrapper) }()
	defer func() {
		server.Close()
		cleanup, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := server.Shutdown(cleanup); err != nil {
			t.Error(err)
		}
	}()
	client := &peerh3.Transport{TLSClientConfig: ct}
	defer client.Close()
	request, _ := http.NewRequestWithContext(ctx, "GET", "https://"+listener.Addr().String()+"/", nil)
	response, err := client.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var requestCtx context.Context
	select {
	case requestCtx = <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	close(fail)
	select {
	case err := <-done:
		if !errors.Is(err, want) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("ServeListener waited for handler")
	}
	if requestCtx.Err() != nil {
		t.Fatal("accept failure canceled active request", requestCtx.Err())
	}
	// Shutdown must still own the worker after ServeListener has returned.
	short, stop := context.WithTimeout(ctx, 20*time.Millisecond)
	err = server.Shutdown(short)
	stop()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("Shutdown lost active worker", err)
	}
	if requestCtx.Err() != nil {
		t.Fatal("shutdown timeout canceled active request")
	}
	close(release)
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != "beforeafter" {
		t.Fatalf("response %q: %v", body, err)
	}
	response.Body.Close()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}
