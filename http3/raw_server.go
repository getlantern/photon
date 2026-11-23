package http3

import (
	"context"
	"sync"

	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/http3conn"
)

// RawServerConn is an HTTP/3 server connection whose QUIC accept loops belong
// to the application. The application accepts every stream and hands HTTP/3
// its own: HandleRequestStream for client-initiated bidirectional streams and
// HandleUnidirectionalStream for unidirectional streams it does not consume
// itself. Create one with Server.NewRawServerConn.
//
// The connection is otherwise served like one passed to ServeQUICConn, and it
// is closed when its HTTP/3 session ends: after Server.Shutdown has drained it,
// on Server.Close, on a protocol error, or after Server.IdleTimeout.
type RawServerConn struct {
	conn     *quic.Conn
	uni      chan *quic.ReceiveStream
	requests chan *handedRequestStream
	drain    <-chan struct{}
	done     chan struct{} // serving ended and every request has been joined
}

// NewRawServerConn starts HTTP/3 on an accepted connection without accepting
// its streams. It validates the configuration and the connection (ALPN h3,
// datagram support) and charges it to MaxConnections; those failures are
// returned and leave conn with the caller. ConnContext then runs on the
// admitted connection: if it returns a canceled context, that error is
// returned and conn is closed, and a nil context panics. The call does not
// block on the peer: the control and QPACK streams are opened in the
// background, and a failure there closes the connection.
func (s *Server) NewRawServerConn(conn *quic.Conn) (*RawServerConn, error) {
	admitted, err := s.admit(context.Background(), conn)
	if err != nil {
		return nil, httpError(err)
	}
	raw := &RawServerConn{conn: conn, uni: make(chan *quic.ReceiveStream), requests: make(chan *handedRequestStream), drain: admitted.entry.drain, done: make(chan struct{})}
	admitted.raw = raw
	go func() {
		defer close(raw.done)
		if err := admitted.serve(); err != nil && s.Logger != nil {
			s.Logger.Debug("http3: handling connection failed", "remote", conn.RemoteAddr(), "error", err)
		}
	}()
	return raw, nil
}

// HandleRequestStream serves the HTTP/3 request on a client-initiated
// bidirectional stream that the application accepted. It returns once the
// request is finished, including a stream its handler took over with
// HTTPStreamer, or the connection has ended. It blocks while
// MaxConcurrentRequests requests are running. After Server.Shutdown the stream
// is reset with H3_REQUEST_REJECTED. The stream's credit is released, as
// HTTP/3 now handles it.
func (c *RawServerConn) HandleRequestStream(str *quic.Stream) {
	str.ReleaseStreamCredit()
	handed := &handedRequestStream{Stream: str, released: make(chan struct{})}
	select {
	case c.requests <- handed:
		select {
		case <-handed.released:
		case <-c.done:
		}
	case <-c.drain:
		str.CancelRead(quic.StreamErrorCode(ErrCodeRequestRejected))
		str.CancelWrite(quic.StreamErrorCode(ErrCodeRequestRejected))
	case <-c.done:
		str.CancelRead(quic.StreamErrorCode(ErrCodeRequestRejected))
		str.CancelWrite(quic.StreamErrorCode(ErrCodeRequestRejected))
	}
}

// HandleUnidirectionalStream gives HTTP/3 a unidirectional stream the
// application accepted, with its stream type not yet read (Peek leaves it in
// place). Every control and QPACK stream must be handed over. A stream of
// unknown type is rejected with H3_STREAM_CREATION_ERROR. The call returns once
// the connection's stream dispatcher has taken the stream, not when the stream
// ends, and releases the stream's credit as HTTP/3 now handles it.
func (c *RawServerConn) HandleUnidirectionalStream(str *quic.ReceiveStream) {
	str.ReleaseStreamCredit()
	select {
	case c.uni <- str:
	case <-c.done:
	}
}

// CloseWithError closes the connection with the given error code and message.
// It does not wait for running handlers, which see their contexts canceled.
func (c *RawServerConn) CloseWithError(code quic.ApplicationErrorCode, msg string) error {
	return c.conn.CloseWithError(code, msg)
}

// AcceptRequest feeds the request loop.
func (c *RawServerConn) AcceptRequest(ctx context.Context) (http3conn.ServerRequestStream, error) {
	stream, err := acceptHanded(ctx, c.conn, c.requests)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

// handedRequestStream tells HandleRequestStream when the request loop has
// finished with its stream.
type handedRequestStream struct {
	*quic.Stream
	released chan struct{}
	once     sync.Once
}

func (s *handedRequestStream) Release(ctx context.Context) error {
	defer s.once.Do(func() { close(s.released) })
	return s.Stream.Release(ctx)
}
func (s *handedRequestStream) QUICStream() *quic.Stream { return s.Stream }

type handedServerTransport struct {
	http3conn.QUICTransport
	streams <-chan *quic.ReceiveStream
}

func (t handedServerTransport) AcceptUni(ctx context.Context) (http3conn.ReceiveStream, error) {
	return acceptHandedUni(ctx, t.Conn, t.streams)
}
