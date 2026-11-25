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

func TestHTTP3PublicServerShutdownTimeoutAndClose(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", uint32(version)), func(t *testing.T) {
			clientTLS, serverTLS := tlsConfigs(t)
			clientTLS.NextProtos = []string{"h3"}
			serverTLS.NextProtos = []string{"h3"}
			listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, &quic.Config{Versions: []quic.Version{version}})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client := &peerh3.Transport{Dial: loopbackDial(t), TLSClientConfig: clientTLS, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}}
			defer client.Close()
			entered := make(chan context.Context, 1)
			handlerDone := make(chan struct{})
			server := &ownh3.Server{MaxConnections: 1, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(handlerDone)
				entered <- r.Context()
				<-r.Context().Done()
			})}
			defer server.Close()
			clientDone := make(chan struct{})
			go func() {
				defer close(clientDone)
				request, _ := http.NewRequestWithContext(ctx, "GET", "https://"+listener.Addr().String()+"/", nil)
				response, err := client.RoundTrip(request)
				if err == nil {
					response.Body.Close()
				}
			}()
			defer func() {
				cancel()
				client.Close()
				select {
				case <-clientDone:
				case <-time.After(time.Second):
					t.Error("HTTP client did not join")
				}
			}()
			conn, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			serveDone := make(chan struct{})
			var serveError error
			go func() { defer close(serveDone); serveError = server.ServeQUICConn(ctx, conn) }()
			defer func() {
				server.Close()
				select {
				case <-serveDone:
				case <-time.After(time.Second):
					t.Error("server did not join")
				}
			}()
			var handlerContext context.Context
			select {
			case handlerContext = <-entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			// Duplicate ownership must not disturb the existing request.
			if err := server.ServeQUICConn(ctx, conn); err == nil {
				t.Fatal("duplicate connection admitted")
			}
			if handlerContext.Err() != nil {
				t.Fatal("duplicate admission canceled handler")
			}
			// A second established connection exceeds admission, but stays caller-owned.
			secondClient, err := quic.DialAddr(ctx, listener.Addr().String(), clientTLS, &quic.Config{Versions: []quic.Version{version}})
			if err != nil {
				t.Fatal(err)
			}
			defer secondClient.CloseWithError(0, "")
			secondServer, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer secondServer.CloseWithError(0, "")
			if err := server.ServeQUICConn(ctx, secondServer); err == nil {
				t.Fatal("connection limit ignored")
			}
			if secondServer.Context().Err() != nil {
				t.Fatal("rejected connection ownership taken")
			}
			shutdownCtx, stopShutdown := context.WithTimeout(ctx, 20*time.Millisecond)
			err = server.Shutdown(shutdownCtx)
			stopShutdown()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("shutdown did not time out", err)
			}
			if handlerContext.Err() != nil {
				t.Fatal("shutdown timeout aborted handler")
			}
			if err := server.ServeQUICConn(ctx, secondServer); !errors.Is(err, ownh3.ErrServerClosed) {
				t.Fatal("admission reopened", err)
			}
			if secondServer.Context().Err() != nil {
				t.Fatal("closed server took rejected connection")
			}
			if err := server.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-handlerDone:
			case <-ctx.Done():
				t.Fatal("Close did not cancel handler")
			}
			select {
			case <-serveDone:
				if serveError == nil {
					t.Fatal("aborted serving reported success")
				}
			case <-ctx.Done():
				t.Fatal("Close did not stop serving")
			}
			if err := server.Shutdown(ctx); err != nil {
				t.Fatal("shutdown after Close", err)
			}
		})
	}
}

func TestHTTP3ListenerShutdownPreservesActiveResponse(t *testing.T) {
	clientTLS, serverTLS := tlsConfigs(t)
	clientTLS.NextProtos = []string{"h3"}
	serverTLS.NextProtos = []string{"h3"}
	listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, &quic.Config{Versions: []quic.Version{quic.Version1}})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	finish := make(chan struct{})
	entered := make(chan context.Context, 1)
	server := &ownh3.Server{MaxConnections: 1, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("before"))
		w.(http.Flusher).Flush()
		entered <- r.Context()
		select {
		case <-finish:
			w.Write([]byte("after"))
		case <-r.Context().Done():
		}
	})}
	defer server.Close()
	serverDone := make(chan struct{})
	var serverError error
	go func() { defer close(serverDone); serverError = server.Serve(ctx, listener) }()
	defer func() {
		server.Close()
		cancel()
		select {
		case <-serverDone:
		case <-time.After(time.Second):
			t.Error("listener did not join")
		}
	}()
	client := &peerh3.Transport{Dial: loopbackDial(t), TLSClientConfig: clientTLS, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version1}}}
	defer client.Close()
	clientDone := make(chan error, 1)
	go func() {
		request, _ := http.NewRequestWithContext(ctx, "GET", "https://"+listener.Addr().String()+"/", nil)
		response, err := client.RoundTrip(request)
		if err != nil {
			clientDone <- err
			return
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err == nil && string(body) != "beforeafter" {
			err = fmt.Errorf("truncated graceful response: %q", body)
		}
		clientDone <- err
	}()
	joinedClient := false
	defer func() {
		cancel()
		client.Close()
		if !joinedClient {
			select {
			case <-clientDone:
			case <-time.After(time.Second):
				t.Error("client did not join")
			}
		}
	}()
	var handlerCtx context.Context
	select {
	case handlerCtx = <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := server.Serve(ctx, listener); err == nil {
		t.Fatal("duplicate listener accepted")
	}
	shutdownCtx, stop := context.WithTimeout(ctx, 20*time.Millisecond)
	err = server.Shutdown(shutdownCtx)
	stop()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("shutdown timeout", err)
	}
	if handlerCtx.Err() != nil {
		t.Fatal("shutdown closed shared socket")
	}
	select {
	case <-serverDone:
		t.Fatal("Serve returned before active response completed")
	default:
	}
	close(finish)
	select {
	case err := <-clientDone:
		joinedClient = true
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-serverDone:
		if !errors.Is(serverError, ownh3.ErrServerClosed) {
			t.Fatal(serverError)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := server.Serve(ctx, listener); !errors.Is(err, ownh3.ErrServerClosed) {
		t.Fatal("server reopened", err)
	}
}
