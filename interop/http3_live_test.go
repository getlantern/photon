package interop

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	h3conn "github.com/getlantern/photon/internal/http3conn"
	h3 "github.com/getlantern/photon/internal/http3wire"
	qpack "github.com/getlantern/photon/internal/qpack"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

// Our public HTTP/3 client connection against a live independent HTTP/3 server.
// Not a claim of full HTTP semantics or dynamic QPACK coverage.
func TestHTTP3LiveIndependentServer(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", uint32(version)), func(t *testing.T) {
			clientTLS, serverTLS := tlsConfigs(t)
			clientTLS.NextProtos = []string{"h3"}
			serverTLS.NextProtos = []string{"h3"}
			payload := bytes.Repeat([]byte("http3-body-"), 1024)
			handled := make(chan error, 1)
			server := &peerh3.Server{TLSConfig: serverTLS, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(io.LimitReader(r.Body, int64(len(payload)+1)))
				if err == nil && (r.Method != "POST" || r.URL.Path != "/echo" || r.URL.Scheme != "" || r.URL.Host != "" || r.Host != "localhost" || r.ProtoMajor != 3 || r.Header.Get("X-Test") != "independent" || !bytes.Equal(body, payload) || r.Trailer.Get("X-Request-Trailer") != "complete") {
					err = fmt.Errorf("request mismatch: %s %s HTTP/%d body=%d trailer=%v", r.Method, r.URL.Path, r.ProtoMajor, len(body), r.Trailer)
				}
				if err != nil {
					handled <- err
					http.Error(w, "invalid request", 400)
					return
				}
				w.WriteHeader(http.StatusEarlyHints)
				w.Header().Set("X-Peer", "upstream-http3")
				w.Header().Set("Content-Length", strconv.Itoa(len(body)))
				w.Header().Set("Trailer", "X-Response-Trailer")
				_, err = w.Write(body)
				w.Header().Set("X-Response-Trailer", "complete")
				handled <- err
			})}
			socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			served := make(chan error, 1)
			go func() { served <- server.Serve(socket) }()
			defer func() {
				server.Close()
				socket.Close()
				select {
				case <-served:
				case <-time.After(5 * time.Second):
					t.Error("HTTP/3 server failed to stop")
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			conn, err := quic.DialAddr(ctx, socket.LocalAddr().String(), clientTLS, &quic.Config{Versions: []quic.Version{version}})
			if err != nil {
				t.Fatal(err)
			}
			defer conn.CloseWithError(0, "done")
			informational := 0
			client, err := ownh3.NewClientConn(ctx, conn, "https://localhost", &ownh3.ClientConfig{
				MaxConcurrentRequests: 1,
				PermitsTrailer:        func(name string) bool { return name == "x-request-trailer" },
				Information: func(status int, _ http.Header) error {
					if status != 103 {
						return fmt.Errorf("unexpected informational status %d", status)
					}
					informational++
					return nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			if client.LocalAddr().Network() != conn.LocalAddr().Network() || client.LocalAddr().String() != conn.LocalAddr().String() || client.RemoteAddr().Network() != conn.RemoteAddr().Network() || client.RemoteAddr().String() != socket.LocalAddr().String() {
				t.Fatal("client addresses", client.LocalAddr(), client.RemoteAddr())
			}
			request, err := http.NewRequestWithContext(ctx, "POST", "https://localhost/echo", bytes.NewReader(payload))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("X-Test", "independent")
			request.Trailer = http.Header{"X-Request-Trailer": {"complete"}}
			response, err := client.RoundTrip(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			if response.StatusCode != 200 || response.ProtoMajor != 3 || response.ContentLength != int64(len(payload)) || response.Header.Get("X-Peer") != "upstream-http3" || response.TLS == nil {
				t.Fatal(response)
			}
			received, err := io.ReadAll(io.LimitReader(response.Body, int64(len(payload)+1)))
			if err != nil || !bytes.Equal(received, payload) {
				t.Fatal("response body", len(received), err)
			}
			if informational != 1 || response.Trailer.Get("X-Response-Trailer") != "complete" {
				t.Fatal(informational, response.Trailer)
			}
			// EOF is not ownership release: a second request must remain queued
			// until the first response is explicitly closed.
			queueCtx, stopQueue := context.WithTimeout(ctx, 20*time.Millisecond)
			queuedBody := &countedHTTPBody{Reader: bytes.NewReader(payload)}
			queued, _ := http.NewRequestWithContext(queueCtx, "POST", "https://localhost/echo", queuedBody)
			if next, err := client.RoundTrip(queued); !errors.Is(err, context.DeadlineExceeded) || next != nil {
				t.Fatal("request slot released at EOF", next, err)
			}
			stopQueue()
			if queuedBody.closes.Load() != 1 {
				t.Fatal("queued body ownership", queuedBody.closes.Load())
			}
			foreignBody := &countedHTTPBody{Reader: bytes.NewReader(payload)}
			foreign, _ := http.NewRequestWithContext(ctx, "POST", "https://other.example/echo", foreignBody)
			if next, err := client.RoundTrip(foreign); err == nil || next != nil {
				t.Fatal("cross-origin request accepted")
			}
			if foreignBody.closes.Load() != 1 {
				t.Fatal("foreign body ownership")
			}
			if err = response.Body.Close(); err != nil {
				t.Fatal("response cleanup", err)
			}
			select {
			case err := <-handled:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}

// Reverse role: our public HTTP/3 Server against the independent client.
func TestHTTP3LiveIndependentClient(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", uint32(version)), func(t *testing.T) {
			clientTLS, serverTLS := tlsConfigs(t)
			clientTLS.NextProtos = []string{"h3"}
			selectedTLS := serverTLS.Clone()
			selectedTLS.NextProtos = []string{"application-original"}
			serverTLS.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) { return selectedTLS, nil }
			serverTLS = ownh3.ConfigureTLSConfig(serverTLS)
			listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, &quic.Config{Versions: []quic.Version{version}})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			transport := &peerh3.Transport{TLSClientConfig: clientTLS, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}}
			defer transport.Close()
			payloadRepeats := 1024
			payload := bytes.Repeat([]byte("reverse-h3-"), payloadRepeats)
			request, err := http.NewRequestWithContext(ctx, "POST", "https://"+listener.Addr().String()+"/echo", bytes.NewReader(payload))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("X-Test", "upstream-client")
			request.Trailer = http.Header{"X-Request-Trailer": []string{"complete"}}
			const requests = 6
			completed := make(chan error, requests)
			clientDone := make(chan struct{})
			go func() {
				defer close(clientDone)
				var workers sync.WaitGroup
				for range requests {
					workers.Add(1)
					go func() {
						defer workers.Done()
						next := request.Clone(ctx)
						bodyReader, err := request.GetBody()
						next.Body = bodyReader
						if err != nil {
							completed <- err
							return
						}
						response, err := transport.RoundTrip(next)
						if err != nil {
							completed <- err
							return
						}
						defer response.Body.Close()
						body, err := io.ReadAll(io.LimitReader(response.Body, int64(len(payload)+1)))
						if err == nil && (response.StatusCode != 200 || response.ProtoMajor != 3 || response.Header.Get("X-Peer") != "independent-http3" || response.Trailer.Get("X-Response-Trailer") != "complete" || !bytes.Equal(body, payload)) {
							err = fmt.Errorf("response mismatch: status=%d proto=%d bytes=%d trailers=%v", response.StatusCode, response.ProtoMajor, len(body), response.Trailer)
						}
						completed <- err
					}()
				}
				workers.Wait()
			}()
			// Always terminate and join the HTTP client, including on fixture failure.
			defer func() {
				cancel()
				transport.Close()
				select {
				case <-clientDone:
				case <-time.After(5 * time.Second):
					t.Error("HTTP client failed to stop")
				}
			}()
			drain := make(chan struct{})
			var finishedHandlers atomic.Int32
			server := &ownh3.Server{
				MaxConcurrentRequests: 2,
				MaxHeaderBytes:        65536,
				PermitsTrailer:        func(name string) bool { return name == "x-response-trailer" },

				Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != "POST" || r.URL.Path != "/echo" || r.URL.Scheme != "" || r.URL.Host != "" || r.Host == "" || r.ProtoMajor != 3 || r.Header.Get("X-Test") != "upstream-client" || r.TLS == nil || r.RemoteAddr == "" {
						t.Error("request metadata", r)
						panic(http.ErrAbortHandler)
					}
					received, err := io.ReadAll(io.LimitReader(r.Body, int64(len(payload)+1)))
					if err != nil || !bytes.Equal(received, payload) || r.Trailer.Get("X-Request-Trailer") != "complete" {
						t.Error("request body/trailers", len(received), err, r.Trailer)
						panic(http.ErrAbortHandler)
					}
					w.WriteHeader(http.StatusEarlyHints)
					w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
					w.Header().Set("X-Peer", "independent-http3")
					w.Header().Set("Trailer", "X-Response-Trailer")
					if _, err := w.Write(payload); err != nil {
						panic(err)
					}
					w.Header().Set("X-Response-Trailer", "complete")
					if finishedHandlers.Add(1) == requests {
						close(drain)
					}

				}),
			}
			serveCtx, stopServing := context.WithCancel(ctx)
			serverDone := make(chan error, 1)
			go func() {
				serverDone <- server.Serve(serveCtx, listener)
			}()
			shutdownDone := make(chan struct{})
			go func() {
				defer close(shutdownDone)
				select {
				case <-drain:
					if err := server.Shutdown(serveCtx); err != nil && serveCtx.Err() == nil {
						t.Error("shutdown", err)
					}
				case <-serveCtx.Done():
				}
			}()
			serverJoined := false
			defer func() {
				stopServing()
				server.Close()
				<-shutdownDone
				if serverJoined {
					return
				}
				select {
				case <-serverDone:
				case <-time.After(5 * time.Second):
					t.Error("HTTP3 request loop failed to join")
				}
			}()
			for range requests {
				select {
				case err := <-completed:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			select {
			case err := <-serverDone:
				serverJoined = true
				if !errors.Is(err, ownh3.ErrServerClosed) {
					t.Fatal("listener shutdown", err)
				}
			case <-ctx.Done():
				t.Fatal("graceful drain", ctx.Err())
			}

		})
	}
}

// The fixture advertises bounded dynamic capacity. Production handlers own framing
// and lifecycle; these callbacks supply the fixture's control/compression policy.
func startHTTP3Dispatch(t *testing.T, parent context.Context, conn *quic.Conn, peerIsServer bool, local *h3conn.LocalStreams) (*h3conn.SettingsState, *h3conn.FieldEncoder, *h3conn.FieldDecoder) {
	t.Helper()
	return startHTTP3DispatchSettings(t, parent, conn, peerIsServer, local, liveHTTP3Settings())
}

func startHTTP3DispatchSettings(t *testing.T, parent context.Context, conn *quic.Conn, peerIsServer bool, local *h3conn.LocalStreams, localSettings []h3.Setting) (*h3conn.SettingsState, *h3conn.FieldEncoder, *h3conn.FieldDecoder) {
	t.Helper()
	session := startHTTP3Session(t, parent, conn, peerIsServer, local, localSettings)
	encoder, err := session.WaitEncoder(parent)
	if err != nil {
		t.Fatal(err)
	}
	return session.Settings(), encoder, session.Decoder()
}

func startHTTP3Session(t *testing.T, parent context.Context, conn *quic.Conn, peerIsServer bool, local *h3conn.LocalStreams, localSettings []h3.Setting) *h3conn.Session {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	session, err := h3conn.StartSession(ctx, h3conn.QUICTransport{Conn: conn}, local, h3conn.SessionConfig{
		PeerIsServer: peerIsServer, LocalSettings: localSettings, MaxRequests: 16,
		Decoder:          qpack.DecoderLimits{MaxTableCapacity: 4096, MaxBlockedStreams: 8, MaxBufferedBytes: 524288, MaxEncodedSection: 65536, MaxDecodedSection: 65536, MaxFields: 64, MaxInstructionLiteralBytes: 65536},
		Encoder:          h3conn.EncoderLimits{Capacity: 4096, MaxBlocked: 8, MaxSections: 64, MaxReferences: 256, MaxEncodedSection: 65536, MaxFields: 64, MaxInstruction: 65536, MaxFieldSection: 65536},
		MaxActiveStreams: 16, MaxSettingsBytes: 65536, MaxSettingsEntries: 64, MaxEncoderInstruction: 65536,
		Control: func(context.Context, h3.ControlEvent) error { return nil },
	})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		select {
		case <-session.Done():
			session.Stop(context.Canceled)
		case <-time.After(5 * time.Second):
			t.Error("HTTP3 session failed to stop")
		}
	})
	return session
}

func liveHTTP3Settings() []h3.Setting {
	return []h3.Setting{{ID: h3.QPACKMaxTableCapacity, Value: 4096}, {ID: h3.QPACKBlockedStreams, Value: 8}, {ID: h3.MaxFieldSectionSize, Value: 65536}}
}

type countedHTTPBody struct {
	io.Reader
	closes atomic.Int32
}

func (b *countedHTTPBody) Close() error { b.closes.Add(1); return nil }
