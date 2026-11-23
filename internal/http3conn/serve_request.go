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
				task.failOrdinary(cause, code)
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
	var raw *RawStream
	response.acceptTunnel = func() (*ServerTunnel, error) {
		if request.Method != "CONNECT" || tunnel != nil || raw != nil || response.sent || response.finished || response.err != nil {
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
	response.takeOver = func() (*RawStream, error) {
		if raw != nil {
			return raw, nil
		}
		if response.wrote {
			return nil, fmt.Errorf("http3: invalid use of HTTPStream after writing the response body")
		}
		if tunnel != nil || response.finished {
			return nil, fmt.Errorf("http3: response is already complete or a tunnel")
		}
		if response.err != nil {
			return nil, response.err
		}
		transport, ok := stream.(ClientExchangeStream)
		underlying := quicStreamOf(stream)
		if !ok || underlying == nil {
			return nil, fmt.Errorf("http3: stream takeover requires a QUIC stream")
		}
		if task.ctx.Err() != nil {
			return nil, context.Cause(task.ctx)
		}
		taken := newRawStream(transport, underlying, task, config.HandlerError)
		taken.body = body.ReadCloser
		taken.attachFields(fields)
		taken.noWriter = fmt.Errorf("http3: invalid use of Stream.Write before sending the response header")
		// Without a final status no response is ever sent: WriteHeader is
		// unavailable from here on, so the stream stays unwritable.
		if response.status != 0 {
			if request.Method == "CONNECT" && response.status >= 200 && response.status < 300 {
				if err := reader.EstablishTunnel(); err != nil {
					return nil, err
				}
			}
			if err := response.commit(nil); err != nil {
				task.fail(err, InternalError)
				return nil, err
			}
			taken.writer = writer
		}
		if datagramHandler != nil {
			taken.datagrams = datagrams
		}
		// The request may have failed meanwhile, under a read or an observer.
		if err := task.takeOver(taken); err != nil {
			return nil, err
		}
		response.hijacked = true
		raw = taken
		return raw, nil
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
	if raw != nil {
		// The application owns the stream beyond the handler. Its request slot
		// and context last until it finishes both directions.
		raw.wait(ctx)
		if ctx.Err() != nil {
			task.fail(context.Cause(ctx), 0x10c)
		}
		return task.result()
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

// requestTask is one request's failure handling. Its owner decides what a
// stream-level event means: an ordinary request fails as a whole, while a
// stream handed to the application (raw) keeps its directions independent and
// only learns that receiving is over. mu guards that decision. Every site
// that depends on the owner decides and records its decision under mu, so a
// takeover is either before an event, which then gets raw-stream treatment, or
// after it, and then sees its effect: a failed request refuses the takeover, a
// finished receive direction is carried into it. mu is never held across I/O
// or application code; resets and cancellation follow a recorded decision.
type requestTask struct {
	ctx       context.Context
	stream    ServerRequestStream
	abort     func(uint64, string)
	cancel    context.CancelCauseFunc
	abortOnce sync.Once

	mu       sync.Mutex
	err      error      // the request failed
	raw      *RawStream // the application owns the stream
	received bool       // receiving is over; reported to raw once

	// Test seams at the two sides of a read's decision. Nil outside tests.
	beforeDecision, afterDecision func()
}

func (t *requestTask) result() error { t.mu.Lock(); defer t.mu.Unlock(); return t.err }

// owner returns the raw stream that has taken the request over, if any.
func (t *requestTask) owner() *RawStream { t.mu.Lock(); defer t.mu.Unlock(); return t.raw }

// fail fails the whole request, whoever owns the stream: for connection and
// message errors, the handler, the request's context and datagram violations.
func (t *requestTask) fail(err error, fallback uint64) {
	if err == nil {
		return
	}
	err, fallback = t.cause(err, fallback)
	t.mu.Lock()
	first := t.record(err)
	t.mu.Unlock()
	t.apply(err, fallback, first)
}

// failOrdinary fails the request for a stream-level event (the peer reset or
// stopped a direction, a read failed) unless the application owns the stream,
// in which case the event is the application's to see and nothing happens.
func (t *requestTask) failOrdinary(err error, fallback uint64) {
	if err == nil {
		return
	}
	err, fallback = t.cause(err, fallback)
	t.mu.Lock()
	if t.raw != nil {
		t.mu.Unlock()
		return
	}
	first := t.record(err)
	t.mu.Unlock()
	t.apply(err, fallback, first)
}

// readOutcome is the one place the result of a read of the body is acted on,
// for the request body, a raw stream's Read and a read that spans a takeover
// alike. closed says the reader itself closed the body, so the error is its
// own doing. It returns the raw stream that owned the request at the decision.
//
//	deadline                      nothing: the read can be repeated
//	protocol or message error     the request fails, whoever owns it
//	end of body, closed body      receiving is over
//	anything else (peer reset)    ordinary request: it fails; raw: receiving is over
func (t *requestTask) readOutcome(err error, closed bool) *RawStream {
	if err == nil || h3.IsDeadline(err) {
		return t.owner()
	}
	if t.beforeDecision != nil {
		t.beforeDecision()
	}
	var protocol *h3.Error
	var message *MessageError
	fatal := errors.As(err, &protocol) || errors.As(err, &message)
	cause, fallback := t.cause(err, 0x10d)
	t.mu.Lock()
	raw := t.raw
	failing := fatal || raw == nil && err != io.EOF && err != io.ErrClosedPipe && !closed
	first := failing && t.record(cause)
	notify := !t.received
	t.received = true
	t.mu.Unlock()
	if t.afterDecision != nil {
		t.afterDecision()
	}
	if failing {
		t.apply(cause, fallback, first)
	}
	if notify && raw != nil {
		close(raw.received)
	}
	return raw
}

// receiveEnded records that receiving is over for a reason other than a read:
// the body was closed, or reading was canceled.
func (t *requestTask) receiveEnded() {
	t.mu.Lock()
	raw, notify := t.raw, !t.received
	t.received = true
	t.mu.Unlock()
	if notify && raw != nil {
		close(raw.received)
	}
}

// takeOver makes raw the owner. It refuses a request that has already failed,
// and tells raw at once if receiving is already over.
func (t *requestTask) takeOver(raw *RawStream) error {
	t.mu.Lock()
	if t.err != nil {
		err := t.err
		t.mu.Unlock()
		return err
	}
	t.raw = raw
	received := t.received
	t.mu.Unlock()
	if received {
		close(raw.received)
	}
	return nil
}

// cause prefers the reason the request's context ended to the error it produced.
func (t *requestTask) cause(err error, fallback uint64) (error, uint64) {
	if t.ctx.Err() != nil && (errors.Is(err, t.ctx.Err()) || errors.Is(err, context.Cause(t.ctx))) {
		return context.Cause(t.ctx), 0x10c
	}
	return err, fallback
}

// record publishes the first failure. The caller holds mu.
func (t *requestTask) record(err error) bool {
	if t.err != nil {
		return false
	}
	t.err = err
	return true
}

// apply carries out a recorded failure. The failure is published first so that
// Abort, which cancels the parent and wakes the cancellation callback, cannot
// replace protocol evidence.
func (t *requestTask) apply(err error, fallback uint64, first bool) {
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
	// Once the stream is taken over its reads are the raw stream's, which
	// serializes them.
	if raw := b.task.owner(); raw != nil {
		return raw.Read(p)
	}
	n, err := b.ReadCloser.Read(p)
	// The takeover may have happened while this read waited.
	if raw := b.task.readOutcome(err, b.closed.Load()); raw != nil {
		return n, raw.mapError.apply(raw.ended(err))
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
	defer b.task.receiveEnded()
	return b.mapError.apply(b.ReadCloser.Close())
}
