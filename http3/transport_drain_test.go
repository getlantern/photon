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

	quic "github.com/getlantern/photon"
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

// A graceful Shutdown sends GOAWAY and leaves the connection to the client, so
// a response the client has not read yet arrives whole. Once the pool releases
// the drained connection, it closes it, and Shutdown completes. With a short
// drain grace the server closes first, with H3_NO_ERROR; the pool must keep
// the reserved response readable after that close too.
func TestTransportGracefulShutdownDeliversBufferedResponse(t *testing.T) {
	for _, serverCloses := range []bool{false, true} {
		for _, trailers := range []bool{false, true} {
			name := "body"
			if trailers {
				name = "trailers"
			}
			if serverCloses {
				name += "/server-closes"
			}
			t.Run(name, func(t *testing.T) {
				testGracefulShutdownBufferedResponse(t, trailers, serverCloses)
			})
		}
	}
}

func testGracefulShutdownBufferedResponse(t *testing.T, trailers, serverCloses bool) {
	server := &Server{PermitsTrailer: func(string) bool { return true }, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if trailers {
			w.Header().Set("Trailer", "X-Drained")
		}
		io.WriteString(w, "buffered")
		if trailers {
			w.Header().Set("X-Drained", "complete")
		}
	})}
	if serverCloses {
		server.IdleTimeout = 50 * time.Millisecond // also the drain grace
	}
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
	shutdown := make(chan error, 1)
	go func() { shutdown <- server.Shutdown(ctx) }()
	select {
	case <-client.session.GoawayReceived():
	case <-ctx.Done():
		t.Fatal("no GOAWAY", ctx.Err())
	}
	if serverCloses {
		select {
		case <-client.session.Done():
		case <-ctx.Done():
			t.Fatal("server close not observed", ctx.Err())
		}
		// A worker that tears the client down does so promptly after
		// the session stops.
		select {
		case <-entry.done:
		case <-time.After(50 * time.Millisecond):
		}
	}
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != "buffered" {
		t.Fatalf("buffered response lost in graceful shutdown: %q %v", body, err)
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
	select {
	case err := <-shutdown:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Shutdown waited on after the connection closed", ctx.Err())
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

// Close reaches the peer even when the server owns the listener whose socket
// the connection uses: closing the listener first would drop the connection
// silently and leave the client to its idle timeout.
func TestServerCloseReachesPeerBeforeOwnedListenerCloses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server := &Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") })}
	client, origin := dialTestServer(ctx, t, server, nil)
	request, _ := http.NewRequestWithContext(ctx, "GET", origin+"/", nil)
	response, err := client.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	server.Close()
	select {
	case <-client.Context().Done():
	case <-time.After(3 * time.Second): // far below the idle timeout
		t.Fatal("the client never heard the server close")
	}
	var closed *quic.PeerCloseError
	if cause := context.Cause(client.Context()); !errors.As(cause, &closed) || !closed.Application || closed.Code != uint64(ErrCodeNoError) {
		t.Fatal("close cause:", cause)
	}
}

// When Shutdown's deadline expires before a drained client closes, the server
// closes that connection at once rather than leaving it to the drain grace.
func TestShutdownExpiryClosesDrainedConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server := &Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") })}
	defer server.Close()
	client, origin := dialTestServer(ctx, t, server, nil)
	request, _ := http.NewRequestWithContext(ctx, "GET", origin+"/", nil)
	response, err := client.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	// The client never closes. Give the drain time to finish, well inside the
	// grace, then let the deadline expire.
	short, stop := context.WithTimeout(ctx, 300*time.Millisecond)
	defer stop()
	if err := server.Shutdown(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("Shutdown:", err)
	}
	select {
	case <-client.Context().Done():
	case <-time.After(maxDrainGrace / 2):
		t.Fatal("drained connection left open after Shutdown's deadline")
	}
	var closed *quic.PeerCloseError
	if cause := context.Cause(client.Context()); !errors.As(cause, &closed) || !closed.Application || closed.Code != uint64(ErrCodeNoError) {
		t.Fatal("close cause:", cause)
	}
}

// A connection that drains only after Shutdown's deadline has expired is
// closed as soon as it drains, not after the drain grace.
func TestShutdownExpiryClosesLaterDrainedConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entered, finish := make(chan struct{}), make(chan struct{})
	server := &Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-finish
		io.WriteString(w, "late")
	})}
	defer server.Close()
	client, origin := dialTestServer(ctx, t, server, nil)
	responses := make(chan error, 1)
	go func() {
		request, _ := http.NewRequestWithContext(ctx, "GET", origin+"/", nil)
		response, err := client.RoundTrip(request)
		if err == nil {
			_, err = io.Copy(io.Discard, response.Body)
			response.Body.Close()
		}
		responses <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	short, stop := context.WithTimeout(ctx, 100*time.Millisecond)
	defer stop()
	if err := server.Shutdown(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("Shutdown:", err)
	}
	close(finish)
	if err := <-responses; err != nil {
		t.Fatal("response:", err)
	}
	select {
	case <-client.Context().Done():
	case <-time.After(maxDrainGrace / 2):
		t.Fatal("connection drained after the deadline waited for the grace")
	}
}
