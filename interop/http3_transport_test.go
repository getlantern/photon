package interop

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

func TestHTTP3TransportPool(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", uint32(version)), func(t *testing.T) {
			clientTLS, serverTLS := tlsConfigs(t)
			serverTLS.NextProtos = []string{"h3"}
			var mu sync.Mutex
			connections := make(map[string]bool)
			server := &peerh3.Server{TLSConfig: serverTLS, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				connections[r.RemoteAddr] = true
				mu.Unlock()
				io.WriteString(w, "pooled response")
			})}
			socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			served := make(chan struct{})
			go func() { defer close(served); server.Serve(socket) }()
			defer func() {
				server.Close()
				socket.Close()
				select {
				case <-served:
				case <-time.After(time.Second):
					t.Error("peer did not join")
				}
			}()
			transport := &ownh3.Transport{TLSClientConfig: clientTLS, QUICConfig: &quic.Config{Versions: []quic.Version{version}}}
			defer transport.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			origin := "https://" + socket.LocalAddr().String()
			fetch := func() (*http.Response, error) {
				request, _ := http.NewRequestWithContext(ctx, "GET", origin, nil)
				return transport.RoundTrip(request)
			}
			fetchCached := func() (*http.Response, error) {
				request, _ := http.NewRequestWithContext(ctx, "GET", origin, nil)
				return transport.RoundTripOpt(request, ownh3.RoundTripOpt{OnlyCachedConn: true})
			}
			if response, err := fetchCached(); response != nil || !errors.Is(err, ownh3.ErrNoCachedConn) {
				t.Fatal("initial cached miss", response, err)
			}
			results := make(chan error, 6)
			for i := 0; i < 6; i++ {
				go func() {
					response, err := fetch()
					if err == nil {
						var payload []byte
						payload, err = io.ReadAll(response.Body)
						closeErr := response.Body.Close()
						if err == nil {
							err = closeErr
						}
						if err == nil && string(payload) != "pooled response" {
							err = fmt.Errorf("bad response %q", payload)
						}
					}
					results <- err
				}()
			}
			for i := 0; i < 6; i++ {
				if err := <-results; err != nil {
					t.Error(err)
				}
			}
			if t.Failed() {
				return
			}
			count := func() int { mu.Lock(); defer mu.Unlock(); return len(connections) }
			if count() != 1 {
				t.Fatal("concurrent requests used multiple connections", count())
			}
			response, err := fetchCached()
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			transport.CloseIdleConnections()
			payload, err := io.ReadAll(response.Body)
			if err != nil || string(payload) != "pooled response" {
				t.Fatal("idle close interrupted active response", string(payload), err)
			}
			if err := response.Body.Close(); err != nil {
				t.Fatal(err)
			}
			if count() != 1 {
				t.Fatal("active connection replaced")
			}
			transport.CloseIdleConnections()
			if response, err := fetchCached(); response != nil || !errors.Is(err, ownh3.ErrNoCachedConn) {
				t.Fatal("evicted cached miss", response, err)
			}
			response, err = fetch()
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if _, err := io.Copy(io.Discard, response.Body); err != nil {
				t.Fatal(err)
			}
			if err := response.Body.Close(); err != nil {
				t.Fatal(err)
			}
			if count() != 2 {
				t.Fatal("idle connection was not replaced", count())
			}
		})
	}
}

func TestHTTP3TransportReplacesDrainingConnection(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", uint32(version)), func(t *testing.T) {
			clientTLS, serverTLS := tlsConfigs(t)
			serverTLS.NextProtos = []string{"h3"}
			listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, &quic.Config{Versions: []quic.Version{version}})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			finish := make(chan struct{})
			firstServer := &ownh3.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.WriteString(w, "start")
				w.(http.Flusher).Flush()
				select {
				case <-finish:
					io.WriteString(w, "end")
				case <-r.Context().Done():
				}
			})}
			secondServer := &ownh3.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "replacement") })}
			accepted := make(chan struct{})
			served := make(chan struct{})
			go func() {
				defer close(served)
				var workers sync.WaitGroup
				defer workers.Wait()
				for i, server := range []*ownh3.Server{firstServer, secondServer} {
					conn, err := listener.Accept(ctx)
					if err != nil {
						return
					}
					if i == 1 {
						close(accepted)
					}
					workers.Add(1)
					go func() { defer workers.Done(); server.ServeQUICConn(ctx, conn) }()
				}
			}()
			defer func() {
				cancel()
				firstServer.Close()
				secondServer.Close()
				select {
				case <-served:
				case <-time.After(time.Second):
					t.Error("servers did not join")
				}
			}()
			transport := &ownh3.Transport{TLSClientConfig: clientTLS, QUICConfig: &quic.Config{Versions: []quic.Version{version}}, ClientConfig: &ownh3.ClientConfig{MaxConcurrentRequests: 1}}
			defer transport.Close()
			origin := "https://" + listener.Addr().String()
			fetch := func() (*http.Response, error) {
				request, _ := http.NewRequestWithContext(ctx, "GET", origin, nil)
				return transport.RoundTrip(request)
			}
			response, err := fetch()
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			shutdown := make(chan error, 1)
			go func() { shutdown <- firstServer.Shutdown(ctx) }()
			defer func() {
				cancel()
				firstServer.Close()
				select {
				case <-shutdown:
				case <-time.After(time.Second):
					t.Error("shutdown did not join")
				}
			}()
			// A request racing GOAWAY may have reserved the old connection and fail
			// with ErrPeerDraining. It has not been sent; the test issues a fresh GET.
			replacement, err := fetch()
			if errors.Is(err, ownh3.ErrPeerDraining) {
				replacement, err = fetch()
			}
			if err != nil {
				t.Fatal(err)
			}
			defer replacement.Body.Close()
			payload, err := io.ReadAll(replacement.Body)
			if err != nil || string(payload) != "replacement" {
				t.Fatal(string(payload), err)
			}
			if err := replacement.Body.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-accepted:
			default:
				t.Fatal("replacement did not dial")
			}
			close(finish)
			payload, err = io.ReadAll(response.Body)
			if err != nil || string(payload) != "startend" {
				t.Fatal("retirement interrupted active response", string(payload), err)
			}
			if err := response.Body.Close(); err != nil {
				t.Fatal(err)
			}
			// Shutdown is joined by cleanup after the response has completed.
		})
	}
}

func TestHTTP3TransportEvictsLeastRecentlyIdle(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", uint32(version)), func(t *testing.T) {
			clientTLS, serverTLS := tlsConfigs(t)
			serverTLS.NextProtos = []string{"h3"}
			var mu sync.Mutex
			peers := []map[string]bool{make(map[string]bool), make(map[string]bool), make(map[string]bool)}
			origins := make([]string, 3)
			for i := range origins {
				socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				server := &peerh3.Server{TLSConfig: serverTLS, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					peers[i][r.RemoteAddr] = true
					mu.Unlock()
					io.WriteString(w, "ok")
				})}
				served := make(chan struct{})
				go func() { defer close(served); server.Serve(socket) }()
				defer func() {
					server.Close()
					socket.Close()
					select {
					case <-served:
					case <-time.After(time.Second):
						t.Error("peer did not join")
					}
				}()
				origins[i] = "https://" + socket.LocalAddr().String()
			}
			transport := &ownh3.Transport{MaxConnections: 2, TLSClientConfig: clientTLS, QUICConfig: &quic.Config{Versions: []quic.Version{version}}}
			defer transport.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			fetch := func(i int) *http.Response {
				t.Helper()
				request, _ := http.NewRequestWithContext(ctx, "GET", origins[i], nil)
				response, err := transport.RoundTrip(request)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { response.Body.Close() })
				payload, err := io.ReadAll(response.Body)
				if err != nil || string(payload) != "ok" {
					t.Fatal(string(payload), err)
				}
				return response
			}
			for _, i := range []int{0, 1, 0, 2, 0} {
				if err := fetch(i).Body.Close(); err != nil {
					t.Fatal(err)
				}
			}
			mu.Lock()
			counts := []int{len(peers[0]), len(peers[1]), len(peers[2])}
			mu.Unlock()
			if counts[0] != 1 || counts[1] != 1 || counts[2] != 1 {
				t.Fatal("recent connection was evicted", counts)
			}
			if err := fetch(1).Body.Close(); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			replaced := len(peers[1])
			mu.Unlock()
			if replaced != 2 {
				t.Fatal("least recently idle connection was retained", replaced)
			}
			// EOF alone still reserves both entries. Capacity admission must not
			// evict either response, and rejection must close the new request body.
			first, second := fetch(0), fetch(1)
			body := &countedHTTPBody{Reader: strings.NewReader("unsent")}
			request, _ := http.NewRequestWithContext(ctx, "POST", origins[2], body)
			if _, err := transport.RoundTrip(request); !errors.Is(err, ownh3.ErrConnectionLimit) {
				t.Fatal(err)
			}
			if body.closes.Load() != 1 {
				t.Fatal("rejected body ownership")
			}
			if err := first.Body.Close(); err != nil {
				t.Fatal(err)
			}
			if err := second.Body.Close(); err != nil {
				t.Fatal(err)
			}
			if err := fetch(2).Body.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestHTTP3TransportIdleTimeout(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, disabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/disabled=%v", uint32(version), disabled), func(t *testing.T) {
				clientTLS, serverTLS := tlsConfigs(t)
				serverTLS.NextProtos = []string{"h3"}
				accepted := make(chan *upstream.Conn, 4)
				server := &peerh3.Server{TLSConfig: serverTLS, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}, ConnContext: func(ctx context.Context, c *upstream.Conn) context.Context { accepted <- c; return ctx }, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "idle") })}
				socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				served := make(chan struct{})
				go func() { defer close(served); server.Serve(socket) }()
				defer func() {
					server.Close()
					socket.Close()
					select {
					case <-served:
					case <-time.After(time.Second):
						t.Error("peer did not join")
					}
				}()
				idle := 100 * time.Millisecond
				if disabled {
					idle = -1
				}
				transport := &ownh3.Transport{IdleConnTimeout: idle, TLSClientConfig: clientTLS, QUICConfig: &quic.Config{Versions: []quic.Version{version}}}
				defer transport.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				fetch := func() *http.Response {
					t.Helper()
					request, _ := http.NewRequestWithContext(ctx, "GET", "https://"+socket.LocalAddr().String(), nil)
					response, err := transport.RoundTrip(request)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { response.Body.Close() })
					payload, err := io.ReadAll(response.Body)
					if err != nil || string(payload) != "idle" {
						t.Fatal(string(payload), err)
					}
					return response
				}
				first := fetch()
				conn := <-accepted
				if err := first.Body.Close(); err != nil {
					t.Fatal(err)
				}
				second := fetch()
				// A new reservation must invalidate the previous idle period, even
				// though its response has already reached EOF.
				select {
				case <-conn.Context().Done():
					t.Fatal("active response connection expired")
				case <-time.After(2 * 100 * time.Millisecond):
				}
				select {
				case <-accepted:
					t.Fatal("connection not reused")
				default:
				}
				if err := second.Body.Close(); err != nil {
					t.Fatal(err)
				}
				if disabled {
					select {
					case <-conn.Context().Done():
						t.Fatal("disabled idle timeout expired")
					case <-time.After(2 * 100 * time.Millisecond):
					}
					transport.CloseIdleConnections()
				}
				select {
				case <-conn.Context().Done():
				case <-ctx.Done():
					t.Fatal("idle connection did not close", ctx.Err())
				}
				third := fetch()
				select {
				case next := <-accepted:
					if next == conn {
						t.Fatal("expired connection reused")
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if err := third.Body.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
