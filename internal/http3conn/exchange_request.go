package http3conn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"

	quic "github.com/getlantern/photon"
	h3 "github.com/getlantern/photon/internal/http3wire"
)

// ClientExchangeStream supplies the send-side context in addition to duplex I/O.
// Its context distinguishes a peer STOP_SENDING from normal local send completion.
type ClientExchangeStream interface {
	ServerRequestStream
	Context() context.Context
}
type ClientExchangeConfig struct {
	Capsules             bool // explicit extension selection, independent of header presence
	RequireHTTPDatagrams bool // checked by Session before request HEADERS
	ExtendedConnect      bool // session-authenticated peer permission
	// AllowConnect requires an endpoint owning duplex tunnel upload and cleanup.
	AllowConnect      bool
	Encoder           *FieldEncoder
	Decoder           *FieldDecoder
	MaxEncodedHeaders uint64
	MaxInformational  int
	PermitsTrailer    func(string) bool
	Information       func(int, http.Header) error
	// Abort must close the connection and interrupt shared QPACK writes. It must
	// not join this exchange synchronously. Done releases endpoint ownership (for
	// example a request lease); it runs once after upload/response cleanup.
	Abort func(uint64, string)
	Done  func()
	// RejectedBeforeSend is called synchronously when Session refuses admission
	// due to GOAWAY, before any request bytes are written.
	RejectedBeforeSend func()
}

var errUploadStopped = errors.New("http3: response body closed; stop upload")

// ExchangeRequest runs upload and response reception concurrently on one fresh
// stream. The caller supplies a context combining request and session/lease
// lifetime. It owns request.Body after argument validation. The returned response
// owns the remaining exchange; its Body MUST be closed, including after EOF.
// Close cancels unfinished upload, interrupts reads, joins upload and releases
// QPACK/endpoint ownership. A peer H3_NO_ERROR STOP_SENDING or connection close
// ends only the upload: a complete early response remains readable while parent
// is not canceled. There are no retries or pooling.
// AllowConnect permits an endpoint-owned duplex upload without trailers; the
// endpoint owns tunnel half-close. Expect/continue scheduling remains endpoint work.
func ExchangeRequest(parent context.Context, request *http.Request, stream ClientExchangeStream, config ClientExchangeConfig) (response *http.Response, err error) {
	if parent == nil || request == nil || stream == nil || config.Encoder == nil || config.Decoder == nil || config.Abort == nil || config.MaxEncodedHeaders == 0 || config.MaxEncodedHeaders > uint64(^uint(0)>>1) || config.MaxInformational < 0 || uint64(stream.StreamID())%4 != 0 {
		return nil, fmt.Errorf("http3: invalid client exchange configuration")
	}
	started := false
	defer func() {
		if !started {
			stream.CancelRead(0x10c)
			stream.CancelWrite(0x10c)
			if request.Body != nil {
				request.Body.Close()
			}
			if config.Done != nil {
				config.Done()
			}
		}
	}()
	outbound, err := HTTPRequestHeaders(request)
	if err != nil {
		return nil, err
	}

	if ExtendedConnectProtocol(request) != "" && !config.ExtendedConnect {
		return nil, ErrExtendedConnectNotNegotiated
	}
	if config.Capsules {
		preflight, err := NewMessage(MessageConfig{Kind: RequestFields, ExtendedConnect: config.ExtendedConnect, Capsules: true})
		if err != nil {
			return nil, err
		}
		if _, _, err := preflight.Headers(outbound); err != nil {
			return nil, err
		}
	}
	if request.Method == "CONNECT" {
		if !config.AllowConnect {
			return nil, fmt.Errorf("http3: CONNECT requires a tunnel owner")
		}
		if len(request.Trailer) != 0 {
			return nil, fmt.Errorf("http3: CONNECT tunnel cannot send trailers")
		}
	}
	effective := *request
	if effective.Method == "" {
		effective.Method = "GET"
	}
	fields, err := NewFieldStream(config.Decoder, uint64(stream.StreamID()))
	if err != nil {
		return nil, err
	}
	reader, err := NewMessageReader(stream, fields, MessageConfig{Kind: ResponseFields, RequestMethod: effective.Method, RequestProtocol: ExtendedConnectProtocol(request), Capsules: config.Capsules}, config.MaxEncodedHeaders)
	if err != nil {
		return nil, err
	}
	writer, err := NewMessageWriter(stream, config.Encoder, uint64(stream.StreamID()), MessageConfig{Kind: RequestFields, ExtendedConnect: config.ExtendedConnect, Capsules: config.Capsules}, config.PermitsTrailer)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancelCause(parent)
	task := &requestTask{ctx: ctx, stream: stream, cancel: cancel, abort: config.Abort}
	uploadCtx, stopUpload := context.WithCancelCause(ctx)
	interrupted := make(chan struct{})
	stopInterrupt := context.AfterFunc(ctx, func() { defer close(interrupted); stream.CancelRead(0x10c); stream.CancelWrite(0x10c) })
	sendStopped := make(chan struct{})
	stopSend := context.AfterFunc(stream.Context(), func() {
		defer close(sendStopped)
		cause := context.Cause(stream.Context())
		var closed *quic.PeerCloseError
		if errors.As(cause, &closed) {
			stopUpload(cause)
		}
		var reset *quic.StreamError
		if errors.As(cause, &reset) && reset.Remote {
			if reset.ErrorCode == 0x100 {
				stopUpload(cause)
			} else {
				task.fail(cause, 0x10c)
			}
		}
	})
	uploadDone := make(chan struct{})
	started = true
	go func() {
		defer close(uploadDone)
		err := WriteHTTPRequest(uploadCtx, &effective, writer, func() { stream.CancelWrite(0x10c) })
		// A failed write can precede stopSend's report of why the peer ended it.
		if err != nil && ctx.Err() == nil && !errors.Is(err, errUploadStopped) && !normalUploadStop(err) && !normalUploadStop(context.Cause(stream.Context())) {
			task.fail(err, 0x10c)
		}
	}()
	var cleanupOnce sync.Once
	var cleanupErr error
	cleanup := func(complete bool) error {
		cleanupOnce.Do(func() {
			stopUpload(errUploadStopped)
			if !complete {
				stream.CancelRead(0x10c)
			}
			<-uploadDone
			if !stopSend() {
				<-sendStopped
			}
			if !stopInterrupt() {
				<-interrupted
			}
			// At validated response EOF all field sections have been decoded and
			// acknowledged. Cancellation is only needed for abandoned input; the
			// peer may already have closed a gracefully drained connection.
			if !complete {
				cleanupErr = fields.Cancel()
			}
			if cleanupErr != nil {
				task.fail(cleanupErr, InternalError)
			}
			if cleanupErr == nil {
				cleanupErr = task.result()
			}
			cancel(context.Canceled)
			if config.Done != nil {
				config.Done()
			}
		})
		return cleanupErr
	}
	transferred := false
	response, err = ReadHTTPResponse(ctx, &effective, reader, HTTPResponseConfig{
		MaxInformational: config.MaxInformational, Information: config.Information,
		Body: BodyConfig{Close: func(complete bool) error {
			if !transferred {
				return nil
			}
			return cleanup(complete)
		}},
	})
	if err != nil {
		err = exchangeFailure(task, err)
		task.fail(err, 0x10c) // apply connection scope before QPACK cleanup can write
		_ = cleanup(false)
		return nil, err
	}
	transferred = true
	response.Request = request
	response.Body = &exchangeResponseBody{ReadCloser: response.Body, task: task}
	return response, nil
}

// An H3_NO_ERROR connection close ends an unfinished upload like the same code
// in STOP_SENDING. Whether the response outlives the connection is the parent
// context's decision (Session keeps accepted responses after a graceful close).
func normalUploadStop(err error) bool {
	var closed *quic.PeerCloseError
	if errors.As(err, &closed) {
		return closed.Application && closed.Code == 0x100
	}
	var reset *quic.StreamError
	return errors.As(err, &reset) && reset.Remote && reset.ErrorCode == 0x100
}

type exchangeResponseBody struct {
	io.ReadCloser
	task   *requestTask
	closed atomic.Bool
}

func (b *exchangeResponseBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if h3.IsDeadline(err) {
		return n, err
	}
	if err != nil && err != io.EOF && !b.closed.Load() {
		err = exchangeFailure(b.task, err)
	}
	if err != nil && err != io.EOF && err != io.ErrClosedPipe {
		var protocol *h3.Error
		var message *MessageError
		if !b.closed.Load() || errors.As(err, &protocol) || errors.As(err, &message) {
			b.task.fail(err, 0x10c)
		}
	}
	return n, err
}
func (b *exchangeResponseBody) Close() error { b.closed.Store(true); return b.ReadCloser.Close() }
func (b *exchangeResponseBody) CloseWithError(code quic.StreamErrorCode, reason string) error {
	b.task.fail(fmt.Errorf("http3: tunnel aborted (%s): %w", reason, &quic.StreamError{StreamID: b.task.stream.StreamID(), ErrorCode: code}), uint64(code))
	return b.Close()
}

func exchangeFailure(task *requestTask, err error) error {
	var protocol *h3.Error
	var message *MessageError
	if errors.As(err, &protocol) || errors.As(err, &message) {
		return err
	}
	if original := task.result(); original != nil {
		return original
	}
	if task.ctx.Err() != nil {
		return context.Cause(task.ctx)
	}
	return err
}
