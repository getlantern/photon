package http3conn

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	h3 "github.com/getlantern/photon/internal/http3wire"
)

type serveStream struct {
	release func(context.Context) error
	source  io.Reader
	messageSink
	mu                    sync.Mutex
	readCodes, writeCodes []quic.StreamErrorCode
	interrupt             func()
}

func (s *serveStream) Read(p []byte) (int, error) { return s.source.Read(p) }
func (s *serveStream) StreamID() quic.StreamID    { return 0 }
func (s *serveStream) CancelRead(code quic.StreamErrorCode) {
	s.mu.Lock()
	s.readCodes = append(s.readCodes, code)
	s.mu.Unlock()
	if s.interrupt != nil {
		s.interrupt()
	}
}
func (s *serveStream) CancelWrite(code quic.StreamErrorCode) {
	s.mu.Lock()
	s.writeCodes = append(s.writeCodes, code)
	s.mu.Unlock()
}
func serveConfig(t *testing.T, handler http.Handler) ServeRequestConfig {
	t.Helper()
	decoder, err := NewFieldDecoder(decoderLimits(), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { decoder.Close(context.Canceled) })
	encoder, err := NewFieldEncoder(SettingsSnapshot{MaxFieldSectionSize: 4096}, encoderLimits(), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	return ServeRequestConfig{Handler: handler, Encoder: encoder, Decoder: decoder, MaxEncodedHeaders: 4096, Abort: func(code uint64, reason string) { t.Errorf("unexpected connection abort %x %s", code, reason) }}
}
func TestServeRequestRoundTrip(t *testing.T) {
	input := readerHeaders(t, requestFields("/echo")...)
	input = append(input, readerFrame(t, h3.Data, []byte("abc"))...)
	stream := &serveStream{source: bytes.NewReader(input)}
	var requestContext context.Context
	config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestContext = r.Context()
		if r.RemoteAddr != "127.0.0.1:1234" || r.TLS == nil || r.TLS.Version != tls.VersionTLS13 {
			t.Error("request metadata", r)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		w.Header().Set("Trailer", "X-Checksum")
		w.WriteHeader(202)
		w.Write(body)
		w.Header().Set("X-Checksum", "ok")
	}))
	config.RemoteAddr = "127.0.0.1:1234"
	config.TLS = &tls.ConnectionState{Version: tls.VersionTLS13}
	config.PermitsTrailer = func(name string) bool { return name == "x-checksum" }
	if err := ServeRequest(context.Background(), stream, config); err != nil {
		t.Fatal(err)
	}
	if requestContext.Err() == nil || stream.closes != 1 || len(stream.writeCodes) != 0 || len(stream.readCodes) != 0 {
		t.Fatal("lifecycle", stream.closes, stream.readCodes, stream.writeCodes)
	}
	response, err := ReadHTTPResponse(context.Background(), &http.Request{Method: "GET"}, responseReader(t, stream.Bytes()), HTTPResponseConfig{Body: BodyConfig{Close: func(bool) error { return nil }}})
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != "abc" || response.StatusCode != 202 || response.Trailer.Get("X-Checksum") != "ok" {
		t.Fatal(string(body), err, response)
	}
}
func TestServeRequestEarlyResponse(t *testing.T) {
	stream := &serveStream{source: bytes.NewReader(readerHeaders(t, requestFields("/")...))}
	config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(403) }))
	if err := ServeRequest(context.Background(), stream, config); err != nil {
		t.Fatal(err)
	}
	if stream.closes != 1 || len(stream.writeCodes) != 0 || len(stream.readCodes) != 1 || stream.readCodes[0] != 0x100 {
		t.Fatal(stream.closes, stream.readCodes, stream.writeCodes)
	}
}
func TestServeRequestFailureScope(t *testing.T) {
	for _, mode := range []string{"panic", "abort-handler", "message", "frame", "ignored-body-error"} {
		t.Run(mode, func(t *testing.T) {
			input := readerHeaders(t, requestFields("/")...)
			code := uint64(InternalError)
			switch mode {
			case "message":
				input = readerHeaders(t, fs(":method", "POST"))
				code = 0x10e
			case "frame":
				input = readerFrame(t, h3.Settings, nil)
				code = h3.FrameUnexpected
			case "ignored-body-error":
				input = append(input, readerFrame(t, h3.Settings, nil)...)
				code = h3.FrameUnexpected
			}
			stream := &serveStream{source: bytes.NewReader(input)}
			config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch mode {
				case "panic":
					panic("broken")
				case "abort-handler":
					panic(http.ErrAbortHandler)
				case "ignored-body-error":
					io.ReadAll(r.Body)
				default:
					t.Error("handler invoked for bad initial headers")
				}
			}))
			abort := uint64(0)
			config.Abort = func(code uint64, _ string) { abort = code }
			err := ServeRequest(context.Background(), stream, config)
			if err == nil || stream.closes != 0 || len(stream.writeCodes) == 0 {
				t.Fatal(err, stream.closes, stream.writeCodes)
			}
			if mode == "frame" || mode == "ignored-body-error" {
				if abort != code {
					t.Fatal("connection scope", abort, code)
				}
			} else if abort != 0 || uint64(stream.writeCodes[0]) != code {
				t.Fatal("stream scope", abort, stream.writeCodes)
			}
			if mode == "panic" {
				var p *HandlerPanicError
				if !errors.As(err, &p) {
					t.Fatal(err)
				}
			}
			if mode == "abort-handler" && !errors.Is(err, http.ErrAbortHandler) {
				t.Fatal(err)
			}
		})
	}
}
func TestServeRequestCancellationUnblocksHeaders(t *testing.T) {
	source, sink := io.Pipe()
	defer source.Close()
	defer sink.Close()
	stream := &serveStream{source: source, interrupt: func() { source.CloseWithError(context.Canceled) }}
	config := serveConfig(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("handler invoked") }))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ServeRequest(ctx, stream, config) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation stuck")
	}
	if stream.closes != 0 || len(stream.writeCodes) == 0 || stream.writeCodes[0] != 0x10c {
		t.Fatal(stream.closes, stream.writeCodes)
	}
}

func TestServeRequestAbortPreservesProtocolError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &serveStream{source: bytes.NewReader(readerFrame(t, h3.Settings, nil))}
	config := serveConfig(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("handler invoked") }))
	config.Abort = func(uint64, string) { cancel() }
	err := ServeRequest(ctx, stream, config)
	var protocol *h3.Error
	if !errors.As(err, &protocol) || protocol.Code != h3.FrameUnexpected {
		t.Fatal("protocol failure masked by cancellation", err)
	}
}
func TestServeRequestHandlerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &serveStream{source: bytes.NewReader(readerHeaders(t, requestFields("/")...))}
	entered := make(chan struct{})
	config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done() }))
	done := make(chan error, 1)
	go func() { done <- ServeRequest(ctx, stream, config) }()
	<-entered
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("handler not canceled")
	}
	if stream.closes != 0 {
		t.Fatal("canceled response sent FIN")
	}
}

type enteredReader struct {
	io.Reader
	entered chan struct{}
	once    sync.Once
}

func (r *enteredReader) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.entered) })
	return r.Reader.Read(p)
}
func TestServeRequestConcurrentBodyCloseKeepsResponse(t *testing.T) {
	source, sink := io.Pipe()
	defer source.Close()
	defer sink.Close()
	waiting := &enteredReader{Reader: source, entered: make(chan struct{})}
	stream := &serveStream{source: io.MultiReader(bytes.NewReader(readerHeaders(t, requestFields("/")...)), waiting), interrupt: func() { source.CloseWithError(context.Canceled) }}
	config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		readDone := make(chan struct{})
		go func() { defer close(readDone); io.ReadAll(r.Body) }()
		<-waiting.entered
		if err := r.Body.Close(); err != nil {
			t.Error(err)
		}
		select {
		case <-readDone:
		case <-time.After(3 * time.Second):
			t.Error("body close did not unblock read")
		}
		w.WriteHeader(403)
	}))
	if err := ServeRequest(context.Background(), stream, config); err != nil {
		t.Fatal(err)
	}
	if stream.closes != 1 || len(stream.writeCodes) != 0 {
		t.Fatal("body-close interruption aborted response", stream.closes, stream.writeCodes)
	}
}

func (s *serveStream) Release(ctx context.Context) error {
	if s.release != nil {
		return s.release(ctx)
	}
	return nil
}

func TestServeRequestTLSStateRefresh(t *testing.T) {
	var requests []*http.Request
	config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests = append(requests, r); w.WriteHeader(204) }))
	state := tls.ConnectionState{Version: tls.VersionTLS13}
	config.TLSState = func() tls.ConnectionState { return state }
	for _, complete := range []bool{false, true} {
		state.HandshakeComplete = complete
		stream := &serveStream{source: bytes.NewReader(readerHeaders(t, requestFields("/")...))}
		if err := ServeRequest(context.Background(), stream, config); err != nil {
			t.Fatal(err)
		}
	}
	if len(requests) != 2 || requests[0].TLS.HandshakeComplete || !requests[1].TLS.HandshakeComplete {
		t.Fatal("TLS state not refreshed per request")
	}
	if requests[0].TLS == requests[1].TLS {
		t.Fatal("request snapshots aliased")
	}
}
