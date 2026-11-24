package interop

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"runtime"
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
			second := make(chan error, 1)
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
			go queuedHTTP3RoundTrip(client, queued, second)
			awaitHTTP3SlotWait(t, ctx)
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

// Close cancels an active upload and thereby frees its slot, so the test above
// passes even if slot waiters are never interrupted. A response body the caller
// has not closed keeps the slot, leaving Close itself as the only wakeup.
func TestHTTP3ClientCloseWakesRequestBehindHeldResponse(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", uint32(version)), func(t *testing.T) {
			clientTLS, serverTLS := tlsConfigs(t)
			serverTLS.NextProtos = []string{"h3"}
			entered := make(chan struct{}, 2)
			server := &peerh3.Server{TLSConfig: serverTLS, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				entered <- struct{}{}
				w.(http.Flusher).Flush()
				<-r.Context().Done()
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
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			origin := "https://" + socket.LocalAddr().String()
			client, err := ownh3.Dial(ctx, origin, clientTLS, &quic.Config{Versions: []quic.Version{version}}, &ownh3.ClientConfig{MaxConcurrentRequests: 1})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			request, _ := http.NewRequestWithContext(ctx, "GET", origin+"/held", nil)
			held, err := client.RoundTrip(request)
			if err != nil {
				t.Fatal(err)
			}
			defer held.Body.Close()
			queuedBody := &countedHTTPBody{Reader: strings.NewReader("queued")}
			queued, _ := http.NewRequestWithContext(ctx, "POST", origin+"/queued", queuedBody)
			result := make(chan error, 1)
			go queuedHTTP3RoundTrip(client, queued, result)
			joined := false
			defer func() {
				held.Body.Close()
				cancel()
				if !joined {
					select {
					case <-result:
					case <-time.After(time.Second):
						t.Error("queued request did not join")
					}
				}
			}()
			awaitHTTP3SlotWait(t, ctx)
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-result:
				joined = true
				if !errors.Is(err, ownh3.ErrClientClosed) {
					t.Fatal("queued close cause", err)
				}
			case <-ctx.Done():
				t.Fatal("queued request stuck behind held response", ctx.Err())
			}
			if queuedBody.closes.Load() != 1 {
				t.Fatal("queued body ownership", queuedBody.closes.Load())
			}
			if _, err := io.ReadAll(held.Body); !errors.Is(err, ownh3.ErrClientClosed) {
				t.Fatal("held response close cause", err)
			}
			<-entered
			select {
			case <-entered:
				t.Fatal("queued request reached peer")
			default:
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
			// delivered closes once the peer has acknowledged the whole response.
			// The handler takes its stream over to learn that; the client sees an
			// ordinary response either way.
			delivered := make(chan struct{})
			server := &ownh3.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				entered <- struct{}{}
				w.WriteHeader(http.StatusOK)
				stream, err := w.(ownh3.HTTPStreamer).HTTPStream()
				if err != nil {
					t.Error(err)
					return
				}
				defer stream.CancelRead(quic.StreamErrorCode(ownh3.ErrCodeNoError))
				stream.Write([]byte("start"))
				select {
				case <-finish:
					stream.Write([]byte("end"))
					if stream.Close() == nil && stream.QUICStream().WaitAcknowledged(r.Context()) == nil {
						close(delivered)
					}
				case <-r.Context().Done():
					stream.CancelWrite(quic.StreamErrorCode(ownh3.ErrCodeRequestCanceled))
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
			go queuedHTTP3RoundTrip(client, queued, result)
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
			awaitHTTP3SlotWait(t, ctx)
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
				// A peer that closes as soon as its response is delivered: photon's
				// server leaves closing to the client after GOAWAY, so close it here.
				select {
				case <-delivered:
				case <-ctx.Done():
					t.Fatal("response never delivered", ctx.Err())
				}
				server.Close()
				select {
				case err := <-shutdown:
					shutdownJoined = true
					if err != nil {
						t.Fatal("shutdown", err)
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
			// Its requests done, the client closes, which completes Shutdown.
			client.Close()
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

func queuedHTTP3RoundTrip(client *ownh3.ClientConn, request *http.Request, result chan<- error) {
	response, err := client.RoundTrip(request)
	if response != nil {
		response.Body.Close()
	}
	result <- err
}

// awaitHTTP3SlotWait returns once the queuedHTTP3RoundTrip goroutine is parked
// in a select made directly by the client package. With the handshake complete
// and every slot taken, that is the slot wait. No public API reports this
// state, so it is read from the goroutine dump instead of assumed after a delay.
func awaitHTTP3SlotWait(t *testing.T, ctx context.Context) {
	t.Helper()
	pkg := reflect.TypeFor[ownh3.ClientConn]().PkgPath() + "."
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n == len(buf) {
			buf = make([]byte, 2*len(buf))
			continue
		}
		for _, goroutine := range strings.Split(string(buf[:n]), "\n\n") {
			header, frames, _ := strings.Cut(goroutine, "\n")
			_, state, _ := strings.Cut(header, "[")
			if strings.HasPrefix(state, "select") && strings.HasPrefix(frames, pkg) && strings.Contains(frames, ".queuedHTTP3RoundTrip(") {
				return
			}
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("request did not reach the slot wait", ctx.Err())
		}
	}
}
