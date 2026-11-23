package http3

import (
	"context"
	"errors"
	"sync"

	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/http3conn"
)

// clientOwnership serializes shutdown with transfer to a replacement HTTP
// session. A released owner can never close the replacement's QUIC connection.
// Release is only safe after all of the old session's critical workers joined.
type clientOwnership struct {
	mu               sync.Mutex
	closeConn        func(uint64, string) error
	closed, released bool
}

func (o *clientOwnership) close(reason string) { _ = o.abort(0x100, reason) }

func (o *clientOwnership) abort(code uint64, reason string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || o.released {
		return nil
	}
	o.closed = true
	return o.closeConn(code, reason)
}

func (o *clientOwnership) release() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return ErrClientClosed
	}
	if o.released {
		return nil
	}
	o.released = true
	return nil
}

// retireRejected joins the rejected HTTP generation and gives up its right to
// close QUIC. The caller owns QUIC after success, including closing it if fresh
// session initialization fails. No request is replayed by this operation.
// NextConnection calls this only after the rejected session has joined.
func (c *ClientConn) retireRejected() error {
	select {
	case <-c.session.Done():
	default:
		return errors.New("http3: session has not stopped")
	}
	if _, err := c.session.WaitEncoder(context.Background()); !errors.Is(err, quic.Err0RTTRejected) {
		return errors.New("http3: session was not rejected as early data")
	}
	if err := c.ownership.release(); err != nil {
		return err
	}
	c.stopRequests(quic.Err0RTTRejected)
	c.cancel(quic.Err0RTTRejected)
	<-c.monitorDone
	return nil
}

func (c *ClientConn) stopRequests(cause error) {
	c.requestsMu.Lock()
	defer c.requestsMu.Unlock()
	if c.requestsClosed {
		return
	}
	c.requestsClosed = true
	c.requestsCloseCause = cause
	for request := range c.requests {
		request.cancel(cause)
	}
}

// Request workers and body cleanup can outlive critical-stream teardown. Route
// their connection aborts through the same owner as Close and the monitor so an
// obsolete generation cannot abort a replacement session.
type ownedClientTransport struct {
	http3conn.QUICTransport
	ownership *clientOwnership
}

func (t ownedClientTransport) Abort(code uint64, reason string) error {
	return t.ownership.abort(code, reason)
}

func (c *ClientConn) rejectedEarlySession() bool {
	select {
	case <-c.session.Done():
	default:
		return false
	}
	_, err := c.session.WaitEncoder(context.Background())
	return errors.Is(err, quic.Err0RTTRejected)
}
