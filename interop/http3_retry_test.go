package interop

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

func TestHTTP3TransportGoawayRetry(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, mode := range []string{"replay", "unrepeatable", "disabled", "twice", "body_error", "body_reset_error", "cancel_replay", "close_replay"} {
			t.Run(fmt.Sprintf("%x/%s", uint32(version), mode), func(t *testing.T) {
				bodyFailure := strings.HasPrefix(mode, "body_")
				clientTLS, serverTLS := tlsConfigs(t)
				serverTLS.NextProtos = []string{"h3"}
				listener, err := upstream.ListenAddr("127.0.0.1:0", serverTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}})
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				var accepted, handled atomic.Int32
				server := &peerh3.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					handled.Add(1)
					payload, err := io.ReadAll(r.Body)
					if err != nil && !bodyFailure {
						t.Error(err)
					}
					w.Write(payload)
				})}
				served := make(chan error, 1)
				go func() {
					for attempt := 0; attempt < 2; attempt++ {
						conn, err := listener.Accept(ctx)
						if err != nil {
							served <- err
							return
						}
						accepted.Add(1)
						defer conn.CloseWithError(0, "test finished")
						if bodyFailure || attempt == 1 && mode != "twice" {
							served <- server.ServeQUICConn(conn)
							return
						}
						control, err := conn.OpenUniStreamSync(ctx)
						if err != nil {
							served <- err
							return
						}
						// Empty SETTINGS prohibit dynamic references, then GOAWAY rejects
						// stream zero after bytes arrive but before any application processes it.
						if _, err = control.Write([]byte{0, 4, 0}); err != nil {
							served <- err
							return
						}
						if _, err = conn.AcceptStream(ctx); err != nil {
							served <- err
							return
						}
						if _, err = control.Write([]byte{7, 1, 0}); err != nil {
							served <- err
							return
						}
						select {
						case <-conn.Context().Done():
						case <-ctx.Done():
							conn.CloseWithError(0, "test canceled")
							served <- ctx.Err()
							return
						}
					}
					served <- nil
				}()
				defer func() {
					cancel()
					server.Close()
					listener.Close()
					select {
					case <-served:
					case <-time.After(time.Second):
						t.Error("peer did not join")
					}
				}()
				transport := &ownh3.Transport{DisableRetries: mode == "disabled", TLSClientConfig: clientTLS, QUICConfig: &quic.Config{Versions: []quic.Version{version}}}
				defer transport.Close()
				original := &countedHTTPBody{Reader: strings.NewReader("replay payload")}
				if bodyFailure {
					original.Reader = rejectionReader{reset: mode == "body_reset_error"}
				}
				replacement := &countedHTTPBody{Reader: strings.NewReader("replay payload")}
				requestCtx, cancelRequest := context.WithCancelCause(ctx)
				defer cancelRequest(context.Canceled)
				cancelCause := errors.New("reconstruction canceled")
				request, _ := http.NewRequestWithContext(requestCtx, "POST", "https://"+listener.Addr().String(), original)
				var recreated atomic.Int32
				if mode != "unrepeatable" {
					request.GetBody = func() (io.ReadCloser, error) {
						recreated.Add(1)
						if mode == "cancel_replay" {
							cancelRequest(cancelCause)
							return replacement, errors.New("GetBody also failed")
						}
						if mode == "close_replay" {
							transport.Close()
						}
						return replacement, nil
					}
				}
				response, err := transport.RoundTrip(request)
				if mode == "replay" {
					if err != nil {
						t.Fatal(err)
					}
					defer response.Body.Close()
					payload, err := io.ReadAll(response.Body)
					if err != nil || string(payload) != "replay payload" {
						t.Fatal(string(payload), err)
					}
					if response.Request != request {
						t.Fatal("response request changed")
					}
					if err := response.Body.Close(); err != nil {
						t.Fatal(err)
					}
					if handled.Load() != 1 || accepted.Load() != 2 || recreated.Load() != 1 || replacement.closes.Load() != 1 {
						t.Fatal("replay ownership", handled.Load(), accepted.Load(), recreated.Load(), replacement.closes.Load())
					}
				} else if mode == "cancel_replay" || mode == "close_replay" {
					if response != nil {
						response.Body.Close()
						t.Fatal("unexpected response")
					}
					expected := cancelCause
					if mode == "close_replay" {
						expected = ownh3.ErrTransportClosed
					}
					if !errors.Is(err, expected) {
						t.Fatal("reconstruction interruption cause", err)
					}
					if accepted.Load() != 1 || handled.Load() != 0 || recreated.Load() != 1 || replacement.closes.Load() != 1 {
						t.Fatal("interrupted replay ownership", accepted.Load(), handled.Load(), recreated.Load(), replacement.closes.Load())
					}
				} else {
					if response != nil {
						response.Body.Close()
						t.Fatal("unexpected response")
					}
					if mode == "body_reset_error" {
						var reset *quic.StreamError
						if !errors.As(err, &reset) || !reset.Remote {
							t.Fatal("body reset cause", err)
						}
					} else if !errors.Is(err, ownh3.ErrRequestRejected) {
						t.Fatal("rejection cause", err)
					}
					want := int32(1)
					if mode == "twice" {
						want = 2
					}
					if accepted.Load() != want || !bodyFailure && handled.Load() != 0 || recreated.Load() != want-1 || replacement.closes.Load() != want-1 {
						t.Fatal("retry bound", accepted.Load(), handled.Load(), recreated.Load(), replacement.closes.Load())
					}
				}
				if original.closes.Load() != 1 {
					t.Fatal("original body not closed once", original.closes.Load())
				}
			})
		}
	}
}

// An application error with the same sentinel must not become retry evidence.
type rejectionReader struct{ reset bool }

func (r rejectionReader) Read([]byte) (int, error) {
	if r.reset {
		return 0, &quic.StreamError{Remote: true, ErrorCode: 0x10b}
	}
	return 0, ownh3.ErrRequestRejected
}

func TestHTTP3TransportResetRetry(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, mode := range []string{"reset", "both", "cancel", "twice"} {
			t.Run(fmt.Sprintf("%x/%s", uint32(version), mode), func(t *testing.T) {
				clientTLS, serverTLS := tlsConfigs(t)
				serverTLS.NextProtos = []string{"h3"}
				listener, err := upstream.ListenAddr("127.0.0.1:0", serverTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}})
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				var streams atomic.Int32
				served := make(chan error, 1)
				go func() {
					conn, err := listener.Accept(ctx)
					if err != nil {
						served <- err
						return
					}
					defer conn.CloseWithError(0, "finished")
					control, err := conn.OpenUniStreamSync(ctx)
					if err != nil {
						served <- err
						return
					}
					if _, err = control.Write([]byte{0, 4, 0}); err != nil {
						served <- err
						return
					}
					for i := 0; i < 2; i++ {
						stream, err := conn.AcceptStream(ctx)
						if err != nil {
							served <- err
							return
						}
						streams.Add(1)
						if i == 0 || mode == "twice" {
							code := upstream.StreamErrorCode(0x10b)
							if mode == "cancel" {
								code = 0x10c
							}
							if mode == "both" {
								stream.CancelRead(code)
							}
							stream.CancelWrite(code)
							if mode == "cancel" {
								break
							}
							continue
						}
						if _, err = io.Copy(io.Discard, stream); err != nil {
							served <- err
							return
						}
						// HEADERS with zero dynamic references and static :status=200,
						// followed by a two-byte DATA frame and FIN.
						if _, err = stream.Write([]byte{1, 3, 0, 0, 0xd9, 0, 2, 'o', 'k'}); err != nil {
							served <- err
							return
						}
						if err = stream.Close(); err != nil {
							served <- err
							return
						}
					}
					select {
					case <-conn.Context().Done():
					case <-ctx.Done():
					}
					served <- nil
				}()
				defer func() {
					cancel()
					listener.Close()
					select {
					case err := <-served:
						if err != nil && !errors.Is(err, context.Canceled) {
							t.Error(err)
						}
					case <-time.After(time.Second):
						t.Error("peer not joined")
					}
				}()
				transport := &ownh3.Transport{TLSClientConfig: clientTLS, QUICConfig: &quic.Config{Versions: []quic.Version{version}}}
				defer transport.Close()
				reader, producer := io.Pipe()
				defer producer.Close()
				original := &earlyUploadBody{reader: reader, entered: make(chan struct{}), closed: make(chan struct{})}
				replacement := &countedHTTPBody{Reader: strings.NewReader("replacement")}
				request, _ := http.NewRequestWithContext(ctx, "POST", "https://"+listener.Addr().String(), original)
				var recreated atomic.Int32
				request.GetBody = func() (io.ReadCloser, error) {
					if original.closes.Load() != 1 {
						t.Error("GetBody before original cleanup")
					}
					recreated.Add(1)
					return replacement, nil
				}
				response, err := transport.RoundTrip(request)
				if mode == "reset" || mode == "both" {
					if err != nil {
						t.Fatal(err)
					}
					defer response.Body.Close()
					payload, err := io.ReadAll(response.Body)
					if err != nil || string(payload) != "ok" {
						t.Fatal(string(payload), err)
					}
					if err := response.Body.Close(); err != nil {
						t.Fatal(err)
					}
					if streams.Load() != 2 || recreated.Load() != 1 || replacement.closes.Load() != 1 {
						t.Fatal("replay ownership", streams.Load(), recreated.Load(), replacement.closes.Load())
					}
				} else {
					if response != nil {
						response.Body.Close()
						t.Fatal("unexpected response")
					}
					var reset *quic.StreamError
					if !errors.As(err, &reset) || !reset.Remote {
						t.Fatal("reset cause", err)
					}
					var h3err *ownh3.Error
					if !errors.As(err, &h3err) || !h3err.Remote || uint64(h3err.ErrorCode) != uint64(reset.ErrorCode) || !errors.Is(err, &ownh3.Error{Remote: true, ErrorCode: h3err.ErrorCode}) {
						t.Fatal("HTTP error contract", err)
					}
					want := int32(1)
					if mode == "twice" {
						want = 2
					}
					if streams.Load() != want || recreated.Load() != want-1 || replacement.closes.Load() != want-1 {
						t.Fatal("retry bound", streams.Load(), recreated.Load(), replacement.closes.Load())
					}
				}
				if original.closes.Load() != 1 {
					t.Fatal("original ownership", original.closes.Load())
				}
			})
		}
	}
}
