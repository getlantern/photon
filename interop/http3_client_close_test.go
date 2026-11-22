package interop

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	"github.com/getlantern/photon/internal/transport"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

func TestHTTP3ClientCloseActiveAndQueuedRequests(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", uint32(version)), func(t *testing.T) {
			clientTLS, serverTLS := tlsConfigs(t)
			serverTLS.NextProtos = []string{"h3"}
			entered := make(chan struct{}, 2)
			server := &peerh3.Server{TLSConfig: serverTLS, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { entered <- struct{}{}; io.Copy(io.Discard, r.Body) })}
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
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			origin := "https://" + socket.LocalAddr().String()
			client, err := ownh3.Dial(ctx, origin, clientTLS, &quic.Config{Versions: []quic.Version{version}}, &ownh3.ClientConfig{MaxConcurrentRequests: 1})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			reader, producer := io.Pipe()
			defer reader.Close()
			defer producer.Close()
			body := &earlyUploadBody{reader: reader, entered: make(chan struct{}), closed: make(chan struct{})}
			request, _ := http.NewRequestWithContext(ctx, "POST", origin+"/blocked", body)
			first := make(chan error, 1)
			go func() {
				response, err := client.RoundTrip(request)
				if response != nil {
					response.Body.Close()
				}
				first <- err
			}()
			firstJoined := false
			defer func() {
				client.Close()
				cancel()
				if !firstJoined {
					select {
					case <-first:
					case <-time.After(time.Second):
						t.Error("active request did not join")
					}
				}
			}()
			select {
			case <-body.entered:
			case <-ctx.Done():
				t.Fatal("upload did not block", ctx.Err())
			}
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("peer did not receive headers", ctx.Err())
			}
			queuedBody := &countedHTTPBody{Reader: strings.NewReader("queued")}
			queued, _ := http.NewRequestWithContext(ctx, "POST", origin+"/queued", queuedBody)
			second, started := make(chan error, 1), make(chan struct{})
			secondJoined := false
			defer func() {
				client.Close()
				cancel()
				if !secondJoined {
					select {
					case <-second:
					case <-time.After(time.Second):
						t.Error("queued request did not join")
					}
				}
			}()
			go func() {
				close(started)
				response, err := client.RoundTrip(queued)
				if response != nil {
					response.Body.Close()
				}
				second <- err
			}()
			<-started
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-first:
				firstJoined = true
				if !errors.Is(err, ownh3.ErrClientClosed) {
					t.Fatal("active close cause", err)
				}
			case <-ctx.Done():
				t.Fatal("active request stuck", ctx.Err())
			}
			select {
			case err := <-second:
				secondJoined = true
				if !errors.Is(err, ownh3.ErrClientClosed) {
					t.Fatal("queued close cause", err)
				}
			case <-ctx.Done():
				t.Fatal("queued request stuck", ctx.Err())
			}
			if body.closes.Load() != 1 || queuedBody.closes.Load() != 1 {
				t.Fatal("body close ownership", body.closes.Load(), queuedBody.closes.Load())
			}
			select {
			case <-entered:
				t.Fatal("queued request reached peer")
			default:
			}
			if err := client.Close(); err != nil {
				t.Fatal("repeated Close", err)
			}
		})
	}
}

func TestHTTP3ClientGoawayWakesQueuedRequest(t *testing.T) {
	testHTTP3ClientGoawayResponse(t, false, false, false, false, false)
}

func TestHTTP3ClientGoawayBufferedResponseAfterClose(t *testing.T) {
	testHTTP3ClientGoawayResponse(t, true, false, false, false, false)
}

func TestHTTP3ClientGoawayCanceledResponseAfterClose(t *testing.T) {
	testHTTP3ClientGoawayResponse(t, true, true, false, false, false)
}

func TestHTTP3ClientExplicitCloseAfterPeerClose(t *testing.T) {
	testHTTP3ClientGoawayResponse(t, true, false, true, false, false)
}

func TestHTTP3ClientLifetimeCanceledAfterPeerClose(t *testing.T) {
	testHTTP3ClientGoawayResponse(t, true, false, false, true, false)
}

func TestHTTP3ClientResponseAfterRuntimeStops(t *testing.T) {
	testHTTP3ClientGoawayResponse(t, true, false, false, false, true)
}

func testHTTP3ClientGoawayResponse(t *testing.T, readAfterClose, cancelRequest, closeClient, cancelLifetime, waitRuntime bool) {
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
			entered := make(chan struct{}, 2)
			server := &ownh3.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				entered <- struct{}{}
				w.Write([]byte("start"))
				w.(http.Flusher).Flush()
				select {
				case <-finish:
					w.Write([]byte("end"))
				case <-r.Context().Done():
				}
			})}
			served := make(chan struct{})
			go func() { defer close(served); server.Serve(ctx, listener) }()
			defer func() {
				server.Close()
				cancel()
				select {
				case <-served:
				case <-time.After(time.Second):
					t.Error("server did not join")
				}
			}()
			origin := "https://" + listener.Addr().String()
			clientTLS.NextProtos = []string{"h3"}
			raw, err := quic.DialAddr(ctx, listener.Addr().String(), clientTLS, &quic.Config{Versions: []quic.Version{version}})
			if err != nil {
				t.Fatal(err)
			}
			defer raw.CloseWithError(0, "")
			clientLifetime, cancelClientLifetime := context.WithCancelCause(ctx)
			defer cancelClientLifetime(context.Canceled)
			client, err := ownh3.NewClientConn(clientLifetime, raw, origin, &ownh3.ClientConfig{MaxConcurrentRequests: 1})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			requestCtx, cancelActive := context.WithCancel(ctx)
			defer cancelActive()
			request, _ := http.NewRequestWithContext(requestCtx, "GET", origin+"/active", nil)
			response, err := client.RoundTrip(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			queuedBody := &countedHTTPBody{Reader: strings.NewReader("queued")}
			queued, _ := http.NewRequestWithContext(ctx, "POST", origin+"/queued", queuedBody)
			result := make(chan error, 1)
			go func() {
				next, err := client.RoundTrip(queued)
				if next != nil {
					next.Body.Close()
				}
				result <- err
			}()
			joined := false
			defer func() {
				client.Close()
				cancel()
				if !joined {
					select {
					case <-result:
					case <-time.After(time.Second):
						t.Error("queued request did not join")
					}
				}
			}()
			shutdown := make(chan error, 1)
			go func() { shutdown <- server.Shutdown(ctx) }()
			shutdownJoined := false
			defer func() {
				server.Close()
				cancel()
				if !shutdownJoined {
					select {
					case <-shutdown:
					case <-time.After(time.Second):
						t.Error("shutdown did not join")
					}
				}
			}()
			select {
			case err := <-result:
				joined = true
				if !errors.Is(err, ownh3.ErrPeerDraining) {
					t.Fatal("queued GOAWAY cause", err)
				}
			case <-ctx.Done():
				t.Fatal("GOAWAY did not release queue", ctx.Err())
			}
			if queuedBody.closes.Load() != 1 {
				t.Fatal("queued body ownership", queuedBody.closes.Load())
			}
			close(finish)
			if readAfterClose {
				select {
				case err := <-shutdown:
					shutdownJoined = true
					if err != nil {
						t.Fatal("shutdown before read", err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				select {
				case <-raw.Context().Done():
				case <-ctx.Done():
					t.Fatal("peer close not observed", ctx.Err())
				}
			}
			if waitRuntime {
				// The connection is already closed, so this queues no data. Poll the
				// actual endpoint state rather than assuming a fixed draining duration.
				// SendDatagram now preserves the terminal connection cause after
				// shutdown, so use the already-closed CloseWithError operation.
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				for !errors.Is(raw.CloseWithError(0, ""), transport.ErrEndpointClosed) {
					select {
					case <-ticker.C:
					case <-ctx.Done():
						t.Fatal("UDP owner did not stop", ctx.Err())
					}
				}
			}
			if cancelRequest {
				cancelActive()
			}
			if closeClient {
				if err := client.Close(); err != nil {
					t.Fatal(err)
				}
			}
			lifetimeCause := errors.New("client lifetime canceled after peer close")
			if cancelLifetime {
				cancelClientLifetime(lifetimeCause)
			}
			payload, err := io.ReadAll(response.Body)
			if cancelLifetime {
				if !errors.Is(err, lifetimeCause) {
					t.Fatal("client lifetime cancellation lost", string(payload), err)
				}
				return
			}
			if closeClient {
				if !errors.Is(err, ownh3.ErrClientClosed) {
					t.Fatal("explicit close lost", string(payload), err)
				}
				return
			}
			if cancelRequest {
				if !errors.Is(err, context.Canceled) {
					t.Fatal("request cancellation lost", string(payload), err)
				}
				return
			}
			if err != nil || string(payload) != "startend" {
				t.Fatal("GOAWAY lost active response", string(payload), err)
			}
			if err := response.Body.Close(); err != nil {
				t.Fatal(err)
			}
			if !shutdownJoined {
				select {
				case err := <-shutdown:
					shutdownJoined = true
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			select {
			case <-entered:
				t.Fatal("queued request was processed")
			default:
			}
		})
	}
}
