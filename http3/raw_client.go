package http3

import (
	"context"
	"fmt"

	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/http3conn"
	h3 "github.com/getlantern/photon/internal/http3wire"
)

// RawClientConn is a ClientConn that leaves the QUIC accept loops to the
// application, so that it can handle streams the server initiates. The
// application accepts every stream and hands HTTP/3 its own:
// HandleUnidirectionalStream for each unidirectional stream it does not consume
// itself, and ClientConn.HandleBidirectionalStream for each bidirectional
// stream its extension does not define. Create one with
// Transport.NewRawClientConn.
type RawClientConn struct {
	*ClientConn
}

// NewClientConn creates an HTTP/3 client connection on conn, a client QUIC
// connection that negotiates ALPN h3 and whose peer the caller authenticated.
// Most callers should use RoundTrip, which dials and pools connections.
//
// It returns at once. HTTP/3 is set up in the background after the handshake,
// so requests never use 0-RTT here; they and ReceivedSettings wait for setup.
// If setup fails, the connection is closed and every later call returns the
// failure. The ClientConn owns conn: it reads the server's unidirectional
// streams, rejects server-initiated bidirectional streams with
// H3_STREAM_CREATION_ERROR, and Close closes conn. Requests are not pinned to
// an origin. The Transport's pool does not track the connection.
func (t *Transport) NewClientConn(conn *quic.Conn) *ClientConn {
	return t.newClientConn(conn, false)
}

// NewRawClientConn is NewClientConn without the accept loops: see RawClientConn.
func (t *Transport) NewRawClientConn(conn *quic.Conn) *RawClientConn {
	return &RawClientConn{ClientConn: t.newClientConn(conn, true)}
}

func (t *Transport) newClientConn(conn *quic.Conn, raw bool) *ClientConn {
	cfg, err := t.clientOptions()
	if err != nil {
		// Admit nothing, but keep the channels valid.
		cfg.MaxConcurrentRequests = 1
	}
	client := newClientShell(context.Background(), conn, "", cfg, false, raw)
	client.ready, client.setupDone, client.settings = make(chan struct{}), make(chan struct{}), make(chan struct{})
	client.autoGzip, client.logger = !t.DisableCompression, t.Logger
	go client.setup(err)
	return client
}

// setup runs in the background for the Transport constructors. It never uses
// early data: SETTINGS remembered for a ticket are only available to DialEarly.
func (c *ClientConn) setup(err error) {
	defer close(c.setupDone)
	if err == nil {
		select {
		case <-c.conn.HandshakeComplete():
		case <-c.ctx.Done():
			err = context.Cause(c.ctx)
		}
	}
	if err == nil && c.config.EnableDatagrams && !c.conn.DatagramsEnabled() {
		err = fmt.Errorf("http3: QUIC datagram reception is disabled")
	}
	if err == nil && c.conn.ConnectionState().TLS.NegotiatedProtocol != NextProtoH3 {
		err = fmt.Errorf("http3: connection did not negotiate h3")
	}
	if err == nil {
		err = c.start(nil)
	}
	if err != nil {
		if c.logger != nil {
			c.logger.Debug("http3: setting up connection failed", "error", err)
		}
		c.setupErr = httpError(err)
		// Abort before canceling: the monitor closes with H3_NO_ERROR as soon as
		// it sees cancellation. A failed critical stream has already closed the
		// connection with its own code.
		_ = c.ownership.abort(http3conn.InternalError, "HTTP3 client initialization failed")
		c.cancel(err)
		close(c.ready)
		return
	}
	close(c.ready)
	forwardSettings(c.session.ReceivedSettings(), c.session.Done(), c.settings)
}

// forwardSettings closes published once received is closed, and gives up when
// the session is done. SETTINGS that arrived before the session ended are still
// published: both channels can be ready at once, and responses kept after a
// graceful close still show them.
func forwardSettings(received, done <-chan struct{}, published chan<- struct{}) {
	select {
	case <-received:
	case <-done:
		select {
		case <-received:
		default:
			return
		}
	}
	close(published)
}

type clientTransport interface {
	http3conn.Transport
	http3conn.IncomingTransport
}

// handedClientTransport reads the unidirectional streams the application hands
// to a raw client instead of accepting them.
type handedClientTransport struct {
	ownedClientTransport
	streams <-chan *quic.ReceiveStream
}

func (t handedClientTransport) AcceptUni(ctx context.Context) (http3conn.ReceiveStream, error) {
	return acceptHandedUni(ctx, t.Conn, t.streams)
}

func acceptHandedUni(ctx context.Context, conn *quic.Conn, streams <-chan *quic.ReceiveStream) (http3conn.ReceiveStream, error) {
	stream, err := acceptHanded(ctx, conn, streams)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

// acceptHanded waits for a stream the application hands over. A closed
// connection reports its own cause, as accepting from it would.

func acceptHanded[S any](ctx context.Context, conn *quic.Conn, streams <-chan S) (stream S, err error) {
	select {
	case stream = <-streams:
		return stream, nil
	case <-conn.Context().Done():
		return stream, context.Cause(conn.Context())
	case <-ctx.Done():
		return stream, context.Cause(ctx)
	}
}

// HandleUnidirectionalStream gives HTTP/3 a unidirectional stream the
// application accepted, with its stream type not yet read (Peek leaves it in
// place). Every control and QPACK stream must be handed over. A stream of
// unknown type is rejected with H3_STREAM_CREATION_ERROR. The call returns once
// the connection's stream dispatcher has taken the stream, not when the stream
// ends, and releases the stream's credit as HTTP/3 now handles it.
func (c *RawClientConn) HandleUnidirectionalStream(str *quic.ReceiveStream) {
	str.ReleaseStreamCredit()
	if c.uni == nil {
		str.CancelRead(quic.StreamErrorCode(ErrCodeStreamCreationError))
		return
	}
	select {
	case c.uni <- str:
	case <-c.ctx.Done():
	}
}

// HandleBidirectionalStream handles a bidirectional stream opened by the
// server. HTTP/3 does not allow servers to open them (RFC 9114 section 6.1), so
// it closes the connection with H3_STREAM_CREATION_ERROR. An application using
// a RawClientConn calls it for the streams its extension does not define.
func (c *ClientConn) HandleBidirectionalStream(str *quic.Stream) {
	reason := fmt.Sprintf("server opened bidirectional stream %d", str.StreamID())
	// Abort first: a failing initialization closes with H3_NO_ERROR as soon as
	// it observes cancellation.
	_ = c.ownership.abort(h3.StreamCreationError, reason)
	c.cancel(&h3.Error{Code: h3.StreamCreationError, Reason: reason})
}
