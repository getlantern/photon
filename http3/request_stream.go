package http3

import (
	"context"
	"fmt"
	"net/http"
	"time"

	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/http3conn"
)

// A RequestStream is a low-level HTTP/3 request stream. It separates sending
// the request from reading the response, so the stream (and its datagrams) can
// be used before the response arrives. It is meant for extensions such as
// WebTransport and the MASQUE proxying protocols.
//
// The stream holds one of the connection's MaxConcurrentRequests slots until
// both directions are finished: Close or CancelWrite, and reading to EOF,
// closing the response body or CancelRead. A GOAWAY that rejects the stream,
// or the end of the connection, resets both directions. One Read and one Write
// may run concurrently. Stream and connection errors are returned as *Error.
type RequestStream struct {
	stream *http3conn.ClientStream
	origin string // the connection's pinned origin; empty if it has none
}

// OpenRequestStream opens a request stream without sending anything on it. It
// waits for the handshake, for a MaxConcurrentRequests slot and for QUIC stream
// credit, bounded by ctx. Raw request streams never use 0-RTT. After a GOAWAY
// it returns ErrPeerDraining.
func (c *ClientConn) OpenRequestStream(ctx context.Context) (*RequestStream, error) {
	stream, err := c.openRequestStream(ctx)
	if err != nil {
		return nil, httpError(err)
	}
	return &RequestStream{stream: stream, origin: c.origin}, nil
}

func (c *ClientConn) openRequestStream(ctx context.Context) (_ *http3conn.ClientStream, err error) {
	if ctx == nil {
		return nil, fmt.Errorf("http3: nil context")
	}
	if err := c.await(ctx); err != nil {
		return nil, err
	}
	select {
	case <-c.conn.HandshakeComplete():
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case <-c.ctx.Done():
		return nil, context.Cause(c.ctx)
	}
	wait, cancel := context.WithCancelCause(ctx)
	interrupted := make(chan struct{})
	go func() {
		defer close(interrupted)
		select {
		case <-c.ctx.Done():
			cancel(context.Cause(c.ctx))
		case <-c.session.GoawayReceived():
			cancel(ErrPeerDraining)
		case <-c.session.Done():
			_, cause := c.session.WaitEncoder(context.Background())
			cancel(cause)
		case <-wait.Done():
		}
	}()
	defer func() { cancel(context.Canceled); <-interrupted }()
	if c.ctx.Err() != nil {
		return nil, context.Cause(c.ctx)
	}
	select {
	case c.slots <- struct{}{}:
	case <-wait.Done():
		return nil, context.Cause(wait)
	}
	// Close waits for the stream's release; register before it can start waiting.
	c.requestsMu.Lock()
	closed, cause := c.requestsClosed, c.requestsCloseCause
	if !closed {
		c.streams.Add(1)
	}
	c.requestsMu.Unlock()
	if closed {
		<-c.slots
		return nil, cause
	}
	release := func() { <-c.slots; c.streams.Done() }
	transferred := false
	defer func() {
		if !transferred {
			release()
		}
	}()
	if _, draining := c.session.Goaway(); draining {
		return nil, ErrPeerDraining
	}
	lifetime, finishLifetime, err := c.requestLifetime(context.Background())
	if err != nil {
		return nil, err
	}
	stream, err := c.conn.OpenStreamSync(wait)
	if err != nil {
		finishLifetime()
		if wait.Err() != nil {
			return nil, context.Cause(wait)
		}
		return nil, err
	}
	var exchange http3conn.ClientExchangeStream = stream
	var bound *http3conn.DatagramStream
	if c.config.EnableDatagrams {
		// The raw API exists for extensions, so the stream has datagram semantics.
		if bound, err = c.session.BindDatagramStream(stream, true); err != nil {
			stream.CancelRead(quic.StreamErrorCode(ErrCodeRequestCanceled))
			stream.CancelWrite(quic.StreamErrorCode(ErrCodeRequestCanceled))
			finishLifetime()
			return nil, err
		}
		exchange = bound
	}
	transferred = true
	return c.session.OpenRequestStream(lifetime, exchange, http3conn.ClientStreamConfig{
		CapsuleProtocols: c.config.CapsuleProtocols, Datagrams: bound, MaxEncodedHeaders: uint64(c.config.responseHeaderLimit()), MaxInformational: c.config.MaxInformational,
		PermitsTrailer: c.config.PermitsTrailer, Information: c.config.Information, MapError: httpError,
		Done: func() {
			if bound != nil {
				bound.DetachDatagrams()
			}
			finishLifetime()
			release()
		},
	})
}

// SendRequestHeader sends the request header. The request must not have a
// body: write it with Write. On a connection pinned to an origin (NewClientConn,
// Dial, DialEarly) a request for any other origin is rejected before anything
// is sent, and the call can be repeated with a valid request. A CONNECT request whose Proto is set to anything
// but "HTTP/1.1" is an Extended CONNECT with that :protocol. The call waits for
// the server's SETTINGS, bounded by the request's context. It may be used once;
// if writing the header fails, the send direction is unusable.
func (s *RequestStream) SendRequestHeader(req *http.Request) error {
	if req == nil {
		return fmt.Errorf("http3: nil request")
	}
	// A connection made for one origin carries no other's requests, here as in
	// RoundTrip. Nothing has been written when this fails.
	if s.origin != "" {
		if key, err := originKey(req.URL); err != nil {
			return err
		} else if key != s.origin {
			return fmt.Errorf("http3: request origin differs from connection origin")
		}
	}
	switch req.Method {
	case MethodGet0RTT, MethodHead0RTT:
		copy := *req
		copy.Method = http.MethodGet
		if req.Method == MethodHead0RTT {
			copy.Method = http.MethodHead
		}
		req = &copy
	case http.MethodConnect:
		if req.Proto != "" && req.Proto != "HTTP/1.1" && http3conn.ExtendedConnectProtocol(req) == "" {
			extended, err := http3conn.WithExtendedConnectProtocol(req, req.Proto)
			if err != nil {
				return err
			}
			req = extended
		}
	}
	return s.stream.SendRequestHeader(req)
}

// ReadResponse reads the response header and returns the final response.
// Interim (1xx) responses are passed to ClientConfig.Information. It does not
// set Response.Request or Response.TLS. The response body is the stream's
// receive direction, the same data Read returns; closing it cancels reading.
// It may be used once, after SendRequestHeader, and again after an expired
// read deadline. Any other failure leaves the receive direction unusable.
func (s *RequestStream) ReadResponse() (*http.Response, error) { return s.stream.ReadResponse() }

// Read reads the response body. It can only be used after ReadResponse.
func (s *RequestStream) Read(b []byte) (int, error) { return s.stream.Read(b) }

// Write sends b in one DATA frame. It can only be used after SendRequestHeader.
// An empty b sends nothing, but still returns the error of a send direction
// that has ended, as TryWriteAll does.
func (s *RequestStream) Write(b []byte) (int, error) { return s.stream.Write(b) }

// TryWriteAll writes b in a DATA frame if the entire frame can be queued
// immediately. Otherwise it returns quic.ErrWouldBlock and queues nothing,
// unless sending has ended, which it reports instead.
// It can only be used after SendRequestHeader.
func (s *RequestStream) TryWriteAll(b []byte) error { return s.stream.TryWriteAll(b) }

// Close closes the send direction of the stream. It does not close the
// receive direction. It fails while a DATA frame interrupted by a write
// deadline is unfinished. It succeeds if the server already stopped the
// direction with H3_NO_ERROR, as it may once it has responded.
func (s *RequestStream) Close() error { return s.stream.Close() }

// CancelRead aborts receiving on this stream, as quic.Stream.CancelRead does.
func (s *RequestStream) CancelRead(errorCode quic.StreamErrorCode) { s.stream.CancelRead(errorCode) }

// CancelWrite aborts sending on this stream, as quic.Stream.CancelWrite does.
func (s *RequestStream) CancelWrite(errorCode quic.StreamErrorCode) { s.stream.CancelWrite(errorCode) }

// Context returns the underlying QUIC stream's context, which is done once
// sending has ended.
func (s *RequestStream) Context() context.Context { return s.stream.Context() }

// StreamID returns the ID of the underlying QUIC stream.
func (s *RequestStream) StreamID() quic.StreamID { return s.stream.StreamID() }

// SetReadDeadline sets the deadline for Read calls. A Read that hits it
// returns os.ErrDeadlineExceeded and can be repeated.
func (s *RequestStream) SetReadDeadline(t time.Time) error { return s.stream.SetReadDeadline(t) }

// SetWriteDeadline sets the deadline for Write calls. A Write that hits it
// returns os.ErrDeadlineExceeded; the next Write first completes its frame.
func (s *RequestStream) SetWriteDeadline(t time.Time) error { return s.stream.SetWriteDeadline(t) }

// SetDeadline sets the read and write deadlines.
func (s *RequestStream) SetDeadline(t time.Time) error { return s.stream.SetDeadline(t) }

// SendDatagram sends an HTTP datagram (RFC 9297) on this request. Datagrams
// must be enabled on the connection and negotiated with the server: before its
// SETTINGS arrive, or without its support, this returns
// ErrHTTPDatagramsNotNegotiated. The request need not have been sent, though a
// server may drop datagrams it cannot associate with a request.
func (s *RequestStream) SendDatagram(b []byte) error { return s.stream.SendDatagram(b) }

// ReceiveDatagram waits for an HTTP datagram sent on this request. Datagrams
// must be enabled on the connection.
func (s *RequestStream) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	return s.stream.ReceiveDatagram(ctx)
}
