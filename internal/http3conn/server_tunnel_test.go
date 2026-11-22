package http3conn

import (
	"bytes"
	"context"
	"errors"
	quic "github.com/getlantern/photon"
	"io"
	"net/http"
	"testing"
	"time"

	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
)

func connectRequestWire(t *testing.T) []byte {
	return readerHeaders(t, qpack.DecodedField{Field: qpack.Field{Name: ":method", Value: "CONNECT"}}, qpack.DecodedField{Field: qpack.Field{Name: ":authority", Value: "target.example:443"}})
}
func TestServeTunnelLifecycle(t *testing.T) {
	input := append(connectRequestWire(t), readerFrame(t, h3.Data, []byte("upload"))...)
	stream := &serveStream{source: bytes.NewReader(input)}
	config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tunnel, err := w.(*HTTPResponseWriter).AcceptTunnel()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write([]byte("illegal writer reuse")); err != io.ErrClosedPipe {
			t.Fatal(err)
		}
		if _, err := w.(*HTTPResponseWriter).AcceptTunnel(); err == nil {
			t.Fatal("repeated handoff")
		}
		if _, err := tunnel.Write([]byte("reply")); err != nil {
			t.Fatal(err)
		}
		if err := tunnel.CloseWrite(); err != nil {
			t.Fatal(err)
		}
		if _, err := tunnel.Write([]byte("after FIN")); err != io.ErrClosedPipe {
			t.Fatal(err)
		}
		// Misuse of the closed send side must not cancel receiving.
		got, err := io.ReadAll(tunnel)
		if err != nil || string(got) != "upload" {
			t.Fatal(string(got), err)
		}
	}))
	if err := ServeRequest(context.Background(), stream, config); err != nil {
		t.Fatal(err)
	}
	if len(stream.readCodes) != 0 || len(stream.writeCodes) != 0 {
		t.Fatal("normal half close reset stream", stream.readCodes, stream.writeCodes)
	}
	frames := h3.NewReader(bytes.NewReader(stream.Bytes()))
	header, err := frames.Next()
	if err != nil || header.Type != h3.Headers {
		t.Fatal(header, err)
	}
	if _, err := io.Copy(io.Discard, frames); err != nil {
		t.Fatal(err)
	}
	header, err = frames.Next()
	if err != nil || header.Type != h3.Data {
		t.Fatal(header, err)
	}
	data, err := io.ReadAll(frames)
	if err != nil || string(data) != "reply" {
		t.Fatal(string(data), err)
	}
}
func TestServeTunnelRejectsInvalidHandoff(t *testing.T) {
	for _, mode := range []string{"get", "sent", "failure-status", "length", "trailer"} {
		t.Run(mode, func(t *testing.T) {
			input := connectRequestWire(t)
			if mode == "get" {
				input = readerHeaders(t, requestFields("/")...)
			}
			stream := &serveStream{source: bytes.NewReader(input)}
			config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch mode {
				case "sent":
					w.(http.Flusher).Flush()
				case "failure-status":
					w.WriteHeader(403)
				case "length":
					w.Header().Set("Content-Length", "0")
				case "trailer":
					w.Header().Set("Trailer", "X-End")
				}
				if tunnel, err := w.(*HTTPResponseWriter).AcceptTunnel(); err == nil || tunnel != nil {
					t.Fatal(tunnel, err)
				}
				w.Header().Del("Content-Length")
				w.Header().Del("Trailer")
			}))
			if err := ServeRequest(context.Background(), stream, config); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestServeTunnelRejectsTrailersAfterAcceptance(t *testing.T) {
	input := append(connectRequestWire(t), readerHeaders(t, qpack.DecodedField{Field: qpack.Field{Name: "x-end", Value: "late"}})...)
	stream := &serveStream{source: bytes.NewReader(input)}
	config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tunnel, err := w.(*HTTPResponseWriter).AcceptTunnel()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadAll(tunnel); err == nil {
			t.Fatal("tunnel accepted trailers")
		}
	}))
	var abort uint64
	config.Abort = func(code uint64, _ string) { abort = code }
	err := ServeRequest(context.Background(), stream, config)
	var protocol *h3.Error
	if !errors.As(err, &protocol) || protocol.Code != h3.FrameUnexpected || abort != h3.FrameUnexpected {
		t.Fatal(err, abort)
	}
}

func TestServeTunnelCloseInterruptsRead(t *testing.T) {
	input, output := io.Pipe()
	defer output.Close()
	stream := &serveStream{source: io.MultiReader(bytes.NewReader(connectRequestWire(t)), input), interrupt: func() { input.CloseWithError(io.ErrClosedPipe) }}
	config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tunnel, err := w.(*HTTPResponseWriter).AcceptTunnel()
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { _, err := tunnel.Read(make([]byte, 1)); done <- err }()
		if err := tunnel.Close(); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("read survived close")
			}
		case <-time.After(time.Second):
			t.Fatal("blocked read")
		}
		if _, err := tunnel.Write([]byte("late")); err != io.ErrClosedPipe {
			t.Fatal(err)
		}
	}))
	if err := ServeRequest(context.Background(), stream, config); err != nil {
		t.Fatal(err)
	}
	if len(stream.writeCodes) != 1 || stream.writeCodes[0] != 0x10c {
		t.Fatal(stream.writeCodes)
	}
}

func TestServerTunnelCloseWithError(t *testing.T) {
	for _, code := range []quic.StreamErrorCode{0x10e, 0x33, 0x1234} {
		stream := &serveStream{source: bytes.NewReader(connectRequestWire(t))}
		config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tunnel, err := w.(*HTTPResponseWriter).AcceptTunnel()
			if err != nil {
				t.Fatal(err)
			}
			if err := tunnel.CloseWithError(code, "invalid extension input"); err != nil {
				t.Fatal(err)
			}
			if r.Context().Err() == nil {
				t.Fatal("handler context not canceled")
			}
			if _, err := tunnel.Write([]byte("after abort")); err != io.ErrClosedPipe {
				t.Fatal(err)
			}
			if _, err := tunnel.Read(make([]byte, 1)); err != io.ErrClosedPipe {
				t.Fatal(err)
			}
			// A later extension failure must preserve the original request cause.
			if err := tunnel.CloseWithError(0x4321, "later"); err != nil {
				t.Fatal(err)
			}
		}))
		err := ServeRequest(context.Background(), stream, config)
		var reset *quic.StreamError
		if !errors.As(err, &reset) || reset.ErrorCode != code || reset.Remote {
			t.Fatal(err)
		}
		if len(stream.readCodes) == 0 || stream.readCodes[0] != code || len(stream.writeCodes) == 0 || stream.writeCodes[0] != code {
			t.Fatal(stream.readCodes, stream.writeCodes)
		}
	}
}

func TestServerTunnelCloseAndAbortOrdering(t *testing.T) {
	for _, order := range []string{"close-first", "abort-first", "concurrent"} {
		t.Run(order, func(t *testing.T) {
			stream := &serveStream{source: bytes.NewReader(connectRequestWire(t))}
			config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				tunnel, err := w.(*HTTPResponseWriter).AcceptTunnel()
				if err != nil {
					t.Fatal(err)
				}
				closeNormal := func() { _ = tunnel.Close() }
				closeError := func() { _ = tunnel.CloseWithError(0x10e, "capsule truncated") }
				switch order {
				case "close-first":
					closeNormal()
					closeError()
				case "abort-first":
					closeError()
					closeNormal()
				default:
					start := make(chan struct{})
					done := make(chan struct{}, 2)
					for _, closeFn := range []func(){closeNormal, closeError} {
						go func() { <-start; closeFn(); done <- struct{}{} }()
					}
					close(start)
					<-done
					<-done
				}
				// All later aborts are inert, including a distinct extension error.
				_ = tunnel.CloseWithError(0x4321, "late failure")
			}))
			err := ServeRequest(context.Background(), stream, config)
			if order == "close-first" && err != nil {
				t.Fatal("late abort changed closed request", err)
			}
			if order == "abort-first" && err == nil {
				t.Fatal("lost initial abort")
			}
			if err != nil {
				var reset *quic.StreamError
				if !errors.As(err, &reset) || reset.ErrorCode != 0x10e {
					t.Fatal("wrong winning failure", err)
				}
			}
			if len(stream.writeCodes) != 1 {
				t.Fatal("duplicate or missing write reset", stream.writeCodes)
			}
			expected := quic.StreamErrorCode(0x10c)
			if err != nil {
				expected = 0x10e
			}
			if stream.writeCodes[0] != expected {
				t.Fatal(stream.writeCodes, err)
			}
		})
	}
}
