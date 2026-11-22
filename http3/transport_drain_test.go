package http3

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func listenTestServer(ctx context.Context, t *testing.T, server *Server) string {
	t.Helper()
	origin := httptest.NewTLSServer(nil)
	server.Addr, server.TLSConfig = "127.0.0.1:0", origin.TLS.Clone()
	origin.Close()
	served := make(chan error, 1)
	go func() { served <- server.ListenAndServe() }()
	t.Cleanup(func() { server.Close() })
	for {
		server.mu.Lock()
		for entry := range server.listeners {
			server.mu.Unlock()
			return entry.listener.Addr().String()
		}
		server.mu.Unlock()
		select {
		case err := <-served:
			t.Fatal("startup failed", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}

func TestTransportGracefulPeerCloseKeepsBufferedResponse(t *testing.T) {
	for _, trailers := range []bool{false, true} {
		name := "body"
		if trailers {
			name = "trailers"
		}
		t.Run(name, func(t *testing.T) {
			server := &Server{PermitsTrailer: func(string) bool { return true }, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if trailers {
					w.Header().Set("Trailer", "X-Drained")
				}
				io.WriteString(w, "buffered")
				if trailers {
					w.Header().Set("X-Drained", "complete")
				}
			})}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			address := listenTestServer(ctx, t, server)
			transport := &Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
			defer transport.Close()
			request, _ := http.NewRequestWithContext(ctx, "GET", "https://"+address+"/", nil)
			response, err := transport.RoundTrip(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			transport.mu.Lock()
			var entry *pooledClient
			for candidate := range transport.all {
				entry = candidate
			}
			client := entry.client
			transport.mu.Unlock()
			// GOAWAY, acknowledged response delivery, then H3_NO_ERROR close.
			if err := server.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case <-client.session.Done():
			case <-ctx.Done():
				t.Fatal("peer close not observed", ctx.Err())
			}
			// The pool worker must keep the reserved response alive. A worker that
			// tears the client down does so promptly after the session stops.
			select {
			case <-entry.done:
			case <-time.After(50 * time.Millisecond):
			}
			body, err := io.ReadAll(response.Body)
			if err != nil || string(body) != "buffered" {
				t.Fatalf("buffered response lost after graceful close: %q %v", body, err)
			}
			if trailers && response.Trailer.Get("X-Drained") != "complete" {
				t.Fatal("buffered trailers lost", response.Trailer)
			}
			if err := response.Body.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entry.done:
			case <-ctx.Done():
				t.Fatal("drained connection retained after release", ctx.Err())
			}
			transport.mu.Lock()
			remaining := len(transport.all) + len(transport.pool)
			transport.mu.Unlock()
			if remaining != 0 {
				t.Fatal("drained connection retained")
			}
		})
	}
}

func TestServerPeerCancellationCancelsIdleHandler(t *testing.T) {
	entered, canceled := make(chan struct{}), make(chan struct{})
	// The handler performs no stream I/O, so only its context can wake it.
	server := &Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		close(canceled)
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	address := listenTestServer(ctx, t, server)
	transport := &Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	defer transport.Close()
	requestCtx, abandon := context.WithCancel(ctx)
	defer abandon()
	request, _ := http.NewRequestWithContext(requestCtx, "GET", "https://"+address+"/", nil)
	result := make(chan error, 1)
	go func() {
		response, err := transport.RoundTrip(request)
		if err == nil {
			response.Body.Close()
		}
		result <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("handler not dispatched", ctx.Err())
	}
	abandon()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	// The connection stays open, so only the request reset can cancel the handler.
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("request reset did not cancel the handler context")
	}
}
