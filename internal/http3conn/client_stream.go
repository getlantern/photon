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

// ClientStreamConfig supplies endpoint policy for one raw request stream. Done
// runs once, after both directions have finished and the stream's lease and
// QPACK state are released.
type ClientStreamConfig struct {
	CapsuleProtocols  map[string]bool
	Datagrams         *DatagramStream // the stream's binding, if it has datagram semantics
	MaxEncodedHeaders uint64
	MaxInformational  int
	PermitsTrailer    func(string) bool
	Information       func(int, http.Header) error
	MapError          func(error) error
	Done              func()
}

// ClientStream is a request stream whose request, response and DATA the
// application drives itself. It holds a GOAWAY lease like ExchangeRequest does:
// a GOAWAY that rejects it, or the end of the session, resets both directions.
type ClientStream struct {
	*RawStream
	session *Session
	config  ClientStreamConfig
	ctx     context.Context

	mu        sync.Mutex // guards the fields below
	sent      bool
	responded bool
	request   *http.Request
	fields    *FieldStream
	reader    *MessageReader
	interim   int // interim responses seen, across ReadResponse retries; under readMu

	complete atomic.Bool // the response was read through a valid FIN
}

// OpenRequestStream admits an already allocated request stream without sending
// anything on it. parent bounds the stream's whole life; the owner cancels it to
// abandon the stream. Every failure resets the stream and runs Done.
func (s *Session) OpenRequestStream(parent context.Context, stream ClientExchangeStream, config ClientStreamConfig) (*ClientStream, error) {
	underlying := quicStreamOf(stream)
	if underlying == nil {
		return nil, fmt.Errorf("http3: raw request stream requires a QUIC stream")
	}
	return s.openRequestStream(parent, stream, underlying, config)
}
func (s *Session) openRequestStream(parent context.Context, stream ClientExchangeStream, underlying *quic.Stream, config ClientStreamConfig) (*ClientStream, error) {
	if parent == nil || stream == nil || config.MaxEncodedHeaders == 0 || config.MaxEncodedHeaders > uint64(^uint(0)>>1) || config.MaxInformational < 0 {
		return nil, fmt.Errorf("http3: invalid raw request stream configuration")
	}
	lease, err := s.BeginRequest(uint64(stream.StreamID()))
	if err != nil {
		code := quic.StreamErrorCode(0x10c)
		if errors.Is(err, ErrPeerDraining) || errors.Is(err, ErrRequestLimit) {
			code = 0x10b
		}
		stream.CancelRead(code)
		stream.CancelWrite(code)
		if config.Done != nil {
			config.Done()
		}
		return nil, err
	}
	ctx, cancel := context.WithCancelCause(parent)
	task := &requestTask{ctx: ctx, stream: stream, cancel: cancel, abort: func(code uint64, reason string) {
		s.cancel(&h3.Error{Code: code, Reason: reason})
		s.transport.Abort(code, reason)
	}}
	raw := newRawStream(stream, underlying, task, config.MapError)
	raw.datagrams = config.Datagrams
	raw.noWriter = fmt.Errorf("http3: invalid use of RequestStream.Write before SendRequestHeader")
	raw.noBody = fmt.Errorf("http3: invalid use of RequestStream.Read before ReadResponse")
	// A raw request stream is the application's from the start.
	_ = task.takeOver(raw)
	c := &ClientStream{RawStream: raw, session: s, config: config, ctx: ctx}
	// As for ExchangeRequest: after a validated end of response every section
	// was decoded and acknowledged, so there is no QPACK work to cancel.
	raw.completed = func() { c.complete.Store(true) }
	observed := make(chan struct{})
	observeLease := func() {
		cause := context.Cause(lease.Context())
		// As for ExchangeRequest: a graceful close leaves a buffered response
		// readable, unless a GOAWAY already read rejects this stream.
		if s.keptAfterClose(cause, uint64(stream.StreamID())) {
			return
		}
		cancel(cause)
	}
	stopLease := context.AfterFunc(lease.Context(), func() { defer close(observed); observeLease() })
	go func() {
		raw.wait(ctx)
		if !stopLease() {
			<-observed
		}
		// The lease or the owner ended the stream before the application did.
		if ctx.Err() != nil {
			stream.CancelRead(0x10c)
			stream.CancelWrite(0x10c)
			raw.receiveEnded()
		}
		c.mu.Lock()
		fields := c.fields
		c.mu.Unlock()
		if fields != nil && !c.complete.Load() {
			if err := fields.Cancel(); err != nil {
				task.fail(err, InternalError)
			}
		}
		cancel(context.Canceled)
		lease.Finish()
		if config.Done != nil {
			config.Done()
		}
	}()
	return c, nil
}

// SendRequestHeader writes the request's HEADERS. The request must have no
// body: the application writes DATA itself. Content-Length is sent only for a
// positive ContentLength, which Write and Close then enforce. It waits for the
// server's SETTINGS, bounded by the request's context and the stream's life.
// It may be called once.
func (c *ClientStream) SendRequestHeader(request *http.Request) (err error) {
	defer func() { err = c.mapError.apply(err) }()
	if request == nil {
		return fmt.Errorf("http3: nil request")
	}
	if request.Body != nil && request.Body != http.NoBody {
		return fmt.Errorf("http3: invalid use of RequestStream.SendRequestHeader with a request that has a request body")
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.mu.Lock()
	sent := c.sent
	c.mu.Unlock()
	if sent {
		return fmt.Errorf("http3: invalid duplicate use of RequestStream.SendRequestHeader")
	}
	effective := *request
	if effective.Method == "" {
		effective.Method = "GET"
	}
	// The body comes through Write, so its length is unknown unless declared:
	// without this a bodiless POST would announce a zero length.
	effective.Body = streamedBody{}
	outbound, err := HTTPRequestHeaders(&effective)
	if err != nil {
		return err
	}
	wait, stopWait := context.WithCancelCause(request.Context())
	stop := context.AfterFunc(c.ctx, func() { stopWait(context.Cause(c.ctx)) })
	encoder, err := c.session.WaitEncoder(wait)
	stop()
	if err != nil && wait.Err() != nil {
		err = context.Cause(wait)
	}
	stopWait(nil)
	if err != nil {
		return err
	}
	protocol := ExtendedConnectProtocol(request)
	extended := c.session.settings != nil && c.session.settings.CanSendExtendedConnect()
	if protocol != "" && !extended {
		return ErrExtendedConnectNotNegotiated
	}
	capsules := c.config.CapsuleProtocols[protocol]
	id := uint64(c.stream.StreamID())
	writer, err := NewMessageWriter(c.stream, encoder, id, MessageConfig{Kind: RequestFields, ExtendedConnect: extended, Capsules: capsules}, c.config.PermitsTrailer)
	if err != nil {
		return err
	}
	fields, err := NewFieldStream(c.session.decoder, id)
	if err != nil {
		return err
	}
	reader, err := NewMessageReader(c.stream, fields, MessageConfig{Kind: ResponseFields, RequestMethod: effective.Method, RequestProtocol: protocol, Capsules: capsules}, c.config.MaxEncodedHeaders)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.sent, c.request, c.fields, c.reader = true, &effective, fields, reader
	c.mu.Unlock()
	c.attachFields(fields)
	// A failed HEADERS write is terminal for sending, and Write reports it.
	c.writer = writer
	if err := writer.Headers(outbound); err != nil {
		var protocol *h3.Error
		if errors.As(err, &protocol) {
			c.task.fail(err, InternalError)
		}
		return err
	}
	return nil
}

// ReadResponse reads through the final response header; interim responses go to
// the configured Information callback. The response body is this stream's
// receive direction: closing it cancels reading. It may be called once, after
// SendRequestHeader, and again after an expired read deadline, which keeps
// what was read. Any other failure ends the receive direction.
func (c *ClientStream) ReadResponse() (response *http.Response, err error) {
	defer func() { err = c.mapError.apply(err) }()
	c.readMu.Lock()
	defer c.readMu.Unlock()
	c.mu.Lock()
	sent, responded, request, reader := c.sent, c.responded, c.request, c.reader
	c.responded = c.responded || sent
	c.mu.Unlock()
	if !sent {
		return nil, fmt.Errorf("http3: invalid use of RequestStream.ReadResponse before SendRequestHeader")
	}
	if responded {
		return nil, fmt.Errorf("http3: invalid duplicate use of RequestStream.ReadResponse")
	}
	transferred := false
	response, err = ReadHTTPResponse(c.ctx, request, reader, HTTPResponseConfig{
		MaxInformational: c.config.MaxInformational, Information: c.config.Information, Interim: &c.interim,
		Body: BodyConfig{Close: func(complete bool) error {
			// A failure before the body exists is scoped below, where it is known.
			if !transferred {
				return nil
			}
			if complete {
				c.complete.Store(true)
			} else {
				c.stream.CancelRead(0x10c)
			}
			c.receiveEnded()
			return nil
		}},
	})
	if h3.IsDeadline(err) {
		// The reader kept what it had; the call can be repeated.
		c.mu.Lock()
		c.responded = false
		c.mu.Unlock()
		return nil, err
	}
	if err != nil {
		var protocol *h3.Error
		var message *MessageError
		if errors.As(err, &protocol) || errors.As(err, &message) {
			c.task.fail(err, 0x10e)
		} else {
			c.stream.CancelRead(0x10c)
		}
		c.receiveEnded()
		return nil, c.ended(err)
	}
	transferred = true
	response.Request = nil
	c.body = response.Body
	response.Body = &clientStreamBody{stream: c.RawStream, body: response.Body}
	return response, nil
}

// clientStreamBody keeps reads through the response on the stream's own path,
// so they are serialized with Read and end the receive direction the same way.
type clientStreamBody struct {
	stream *RawStream
	body   io.Closer
}

func (b *clientStreamBody) Read(p []byte) (int, error) { return b.stream.Read(p) }
func (b *clientStreamBody) Close() error               { return b.stream.mapError.apply(b.body.Close()) }

// streamedBody stands for DATA the application writes itself. It is never read.
type streamedBody struct{}

func (streamedBody) Read([]byte) (int, error) { return 0, io.EOF }
func (streamedBody) Close() error             { return nil }
