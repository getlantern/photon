package http3conn

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	quic "github.com/getlantern/photon"
	h3 "github.com/getlantern/photon/internal/http3wire"
)

// ServerRequestStream is one accepted client-initiated bidirectional stream.
// Cancellation must interrupt blocked I/O and be safe concurrently with it.
// Release waits for receive cleanup and acknowledged send data/FIN or reset.
type ServerRequestStream interface {
	io.ReadWriteCloser
	StreamID() quic.StreamID
	CancelRead(quic.StreamErrorCode)
	CancelWrite(quic.StreamErrorCode)
	Release(context.Context) error
}

// ServeRequestConfig supplies connection-owned state to a single request task.
// Abort must close the connection and interrupt shared QPACK writes; it must be
// concurrency-safe and must not synchronously join this request task. The owner
// bounds concurrent tasks and joins them before releasing the session/transport.
// Handler must return when its request context is canceled. Extension handshakes,
// Expect handling, deadlines and graceful admission remain endpoint work.
type ServeRequestConfig struct {
	// Priorities coordinates header signals with control updates. ServeRequests
	// registers and releases its bounded per-request state.
	Priorities *RequestPriorities
	// HandlerError adapts errors returned by handler body, response and tunnel I/O.
	// It must be concurrent-safe and preserve underlying causes.
	HandlerError func(error) error

	// HandlerPolicy wraps the selected ordinary or extension handler. Execution
	// remains inside panic recovery and the request cancellation lifetime.
	HandlerPolicy func(http.Handler) http.Handler

	CapsuleProtocols        map[string]bool
	DatagramHandlers        map[string]http.Handler
	ExtendedConnect         bool // permission from our server SETTINGS
	ExtendedConnectHandlers map[string]http.Handler
	Handler                 http.Handler
	Encoder                 *FieldEncoder
	Decoder                 *FieldDecoder
	MaxEncodedHeaders       uint64
	PermitsTrailer          func(string) bool
	RemoteAddr              string
	TLS                     *tls.ConnectionState
	// TLSState returns current metadata at request dispatch, overriding TLS.
	// It must be safe for concurrent requests. Each request retains its own snapshot.
	TLSState func() tls.ConnectionState
	Abort    func(code uint64, reason string)
}

// HandlerPanicError retains the recovered handler value for endpoint logging.
// ErrAbortHandler is returned directly and should not be logged as a panic.
type HandlerPanicError struct{ Value any }

func (e *HandlerPanicError) Error() string { return fmt.Sprintf("http3: handler panic: %v", e.Value) }

// ServeRequest owns request parsing, handler execution, response FIN and receive
// cleanup. It cancels the handler context before returning. Error scope is applied
// even if the handler ignores a body-read error. No request is retried, and a
// failed response is never replaced with an automatic success/error response.
func ServeRequest(parent context.Context, stream ServerRequestStream, config ServeRequestConfig) (result error) {
	if parent == nil || stream == nil || config.Handler == nil || config.Encoder == nil || config.Decoder == nil || config.Abort == nil || uint64(stream.StreamID())%4 != 0 {
		return fmt.Errorf("http3: invalid server request configuration")
	}
	fields, err := NewFieldStream(config.Decoder, uint64(stream.StreamID()))
	if err != nil {
		return err
	}
	reader, err := NewMessageReader(stream, fields, MessageConfig{Kind: RequestFields, ExtendedConnect: config.ExtendedConnect, CapsuleProtocols: config.CapsuleProtocols}, config.MaxEncodedHeaders)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancelCause(parent)
	task := &requestTask{ctx: ctx, stream: stream, abort: config.Abort, cancel: cancel}
	datagrams, _ := stream.(*DatagramStream)
	if datagrams != nil {
		ended := make(chan struct{})
		abortContext := datagrams.DatagramAbortContext()
		stop := context.AfterFunc(abortContext, func() { defer close(ended); task.fail(context.Cause(abortContext), 0x33) })
		defer func() {
			if !stop() {
				<-ended
			}
			if result == nil {
				result = task.result()
			}
		}()
		if abortContext.Err() != nil {
			task.fail(context.Cause(abortContext), 0x33)
		}
	}
	interrupted := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(interrupted); task.fail(context.Cause(ctx), 0x10c) })
	defer func() {
		if !stop() {
			<-interrupted
		}
		if err := fields.Cancel(); err != nil {
			task.fail(err, InternalError)
		}
		cancel(context.Canceled)
		if result == nil {
			result = task.result()
		}
	}()
	// A handler waiting only on its context performs no I/O that would surface
	// a peer RESET_STREAM or STOP_SENDING. Request FIN and local completion are
	// not StreamErrors from the peer and leave the task running.
	observe := func(completion context.Context, code uint64) func() {
		ended := make(chan struct{})
		stop := context.AfterFunc(completion, func() {
			defer close(ended)
			var reset *quic.StreamError
			if cause := context.Cause(completion); errors.As(cause, &reset) && reset.Remote {
				task.fail(cause, code)
			}
		})
		return func() {
			if !stop() {
				<-ended
			}
		}
	}
	if receiving, ok := stream.(interface{ ReceiveContext() context.Context }); ok {
		defer observe(receiving.ReceiveContext(), 0x10d)()
	}
	if sending, ok := stream.(interface{ Context() context.Context }); ok {
		defer observe(sending.Context(), 0x10c)()
	}
	parsed := false
	request, _, err := ReadHTTPRequest(ctx, reader, BodyConfig{Close: func(complete bool) error {
		if parsed && !complete && task.result() == nil {
			stream.CancelRead(quic.StreamErrorCode(0x100))
		}
		return nil
	}})
	if err != nil {
		task.fail(err, 0x10d)
		return task.result()
	}
	parsed = true
	priority, _ := h3.ParsePriority(strings.Join(request.Header.Values("Priority"), ","))
	if config.Priorities != nil {
		config.Priorities.header(stream, priority)
	} else {
		applyRequestPriority(stream, priority)
	}

	datagramHandler := config.DatagramHandlers[ExtendedConnectProtocol(request)]
	if datagrams != nil {
		if err := datagrams.ResolveDatagrams(datagramHandler != nil); err != nil {
			task.fail(err, 0x33)
			request.Body.Close()
			return task.result()
		}
	}
	request.RemoteAddr = config.RemoteAddr
	if config.TLSState != nil {
		state := config.TLSState()
		request.TLS = &state
	} else if config.TLS != nil {
		state := *config.TLS
		request.TLS = &state
	}
	body := &serverRequestBody{ReadCloser: request.Body, task: task, mapError: config.HandlerError}
	request.Body = body
	defer body.Close()
	writer, err := NewMessageWriter(stream, config.Encoder, uint64(stream.StreamID()), MessageConfig{Kind: ResponseFields, RequestMethod: request.Method, RequestProtocol: ExtendedConnectProtocol(request), Capsules: config.CapsuleProtocols[ExtendedConnectProtocol(request)]}, config.PermitsTrailer)
	if err != nil {
		task.fail(err, InternalError)
		return task.result()
	}
	response, err := NewHTTPResponseWriter(writer)
	if err != nil {
		task.fail(err, InternalError)
		return task.result()
	}
	response.mapError = config.HandlerError
	var tunnel *ServerTunnel
	response.acceptTunnel = func() (*ServerTunnel, error) {
		if request.Method != "CONNECT" || tunnel != nil || response.sent || response.finished || response.err != nil {
			return nil, fmt.Errorf("http3: tunnel requires an uncommitted CONNECT response")
		}
		if response.status != 0 && (response.status < 200 || response.status >= 300) {
			return nil, fmt.Errorf("http3: tunnel requires a successful response")
		}
		headers := response.header
		if response.status != 0 {
			headers = response.snapshot
		}
		for name := range headers {
			if strings.EqualFold(name, "Content-Length") || strings.EqualFold(name, "Trailer") || strings.HasPrefix(name, http.TrailerPrefix) {
				return nil, fmt.Errorf("http3: tunnel prohibits response length and trailers")
			}
		}
		if task.ctx.Err() != nil {
			return nil, context.Cause(task.ctx)
		}
		if err := reader.EstablishTunnel(); err != nil {
			return nil, err
		}
		if err := response.commit(nil); err != nil {
			task.fail(err, InternalError)
			return nil, err
		}
		response.finished = true // only the tunnel may write DATA or FIN after handoff
		tunnel = &ServerTunnel{body: body, writer: writer, task: task, mapError: config.HandlerError}
		if datagramHandler != nil {
			tunnel.datagrams = datagrams
		}
		return tunnel, nil
	}
	if ctx.Err() != nil {
		task.fail(context.Cause(ctx), 0x10c)
	}
	if task.result() == nil {
		handler := config.Handler
		if protocol := ExtendedConnectProtocol(request); protocol != "" {
			handler = datagramHandler
			if handler == nil {
				handler = config.ExtendedConnectHandlers[protocol]
			}
			if handler == nil {
				handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					http.Error(w, "unsupported Extended CONNECT protocol", http.StatusNotImplemented)
				})
			}
		}
		if config.HandlerPolicy != nil {
			handler = config.HandlerPolicy(handler)
		}
		if err := invokeHandler(handler, response, request); err != nil {
			task.fail(err, InternalError)
		}
	}
	if tunnel != nil {
		if task.result() == nil && !tunnel.closed.Load() {
			_ = tunnel.CloseWrite()
		}
		_ = tunnel.Close()
		return task.result()
	}
	if err := response.Err(); err != nil {
		task.fail(err, InternalError)
	}
	// Validate/commit pending response headers before normal receive cleanup can
	// select H3_NO_ERROR. Otherwise a malformed response discovered by Finish
	// would be too late to select the required request reset code.
	if task.result() == nil && !response.sent {
		if err := response.commit(nil); err != nil {
			task.fail(err, InternalError)
		}
	}
	if err := body.Close(); err != nil {
		task.fail(err, InternalError)
	}
	if ctx.Err() != nil {
		task.fail(context.Cause(ctx), 0x10c)
	}
	if task.result() == nil {
		if err := response.Finish(); err != nil {
			task.fail(err, InternalError)
		}
	}

	return task.result()
}

type requestTask struct {
	ctx       context.Context
	stream    ServerRequestStream
	abort     func(uint64, string)
	cancel    context.CancelCauseFunc
	mu        sync.Mutex
	err       error
	abortOnce sync.Once
}

func (t *requestTask) result() error { t.mu.Lock(); defer t.mu.Unlock(); return t.err }
func (t *requestTask) fail(err error, fallback uint64) {
	if err == nil {
		return
	}
	if t.ctx.Err() != nil && (errors.Is(err, t.ctx.Err()) || errors.Is(err, context.Cause(t.ctx))) {
		fallback = 0x10c
		err = context.Cause(t.ctx)
	}
	// Publish the original failure before Abort can cancel the parent and wake
	// the cancellation callback; that callback must not replace protocol evidence.
	t.mu.Lock()
	first := t.err == nil
	if first {
		t.err = err
	}
	t.mu.Unlock()
	var connError *h3.Error
	if errors.As(err, &connError) {
		t.abortOnce.Do(func() { t.abort(connError.Code, connError.Reason) })
	}
	var datagramError *DatagramRequestError
	var streamError *quic.StreamError
	if errors.As(err, &datagramError) || errors.As(err, &streamError) && !streamError.Remote && streamError.ErrorCode == quic.StreamErrorCode(h3.DatagramError) {
		fallback = h3.DatagramError
	}
	var messageError *MessageError
	if errors.As(err, &messageError) {
		fallback = 0x10e
	}
	if first {
		t.stream.CancelRead(quic.StreamErrorCode(fallback))
		t.stream.CancelWrite(quic.StreamErrorCode(fallback))
		t.cancel(err)
	}
}

type serverRequestBody struct {
	mapError handlerErrorMap
	io.ReadCloser
	task   *requestTask
	closed atomic.Bool
}

func (b *serverRequestBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil && err != io.EOF && err != io.ErrClosedPipe {
		var connError *h3.Error
		var messageError *MessageError
		if !b.closed.Load() || errors.As(err, &connError) || errors.As(err, &messageError) {
			b.task.fail(err, 0x10d)
		}
	}
	return n, b.mapError.apply(err)
}
func invokeHandler(handler http.Handler, writer http.ResponseWriter, request *http.Request) (err error) {
	defer func() {
		if value := recover(); value != nil {
			if value == http.ErrAbortHandler {
				err = http.ErrAbortHandler
			} else {
				err = &HandlerPanicError{Value: value}
			}
		}
	}()
	handler.ServeHTTP(writer, request)
	return nil
}

func (b *serverRequestBody) Close() error {
	b.closed.Store(true)
	return b.mapError.apply(b.ReadCloser.Close())
}
