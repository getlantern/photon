package http3

import (
	"context"
	"errors"
	"fmt"

	quic "github.com/getlantern/photon"
)

// These methods opt a request into replayable early data. The wire methods are
// GET and HEAD. Callers must assess replay safety for the selected resource.
const (
	MethodGet0RTT  = "GET_0RTT"
	MethodHead0RTT = "HEAD_0RTT"
)

// NextConnection waits for TLS completion. Accepted early data returns c.
// Rejected early data retires c and initializes a fresh HTTP session on the same
// QUIC connection. It never replays a request. Close the returned client; Close
// on the retired client cannot close its replacement. Concurrent calls share one
// replacement. ctx bounds the wait and setup, not the replacement's lifetime.
func (c *ClientConn) NextConnection(ctx context.Context) (result *ClientConn, err error) {
	defer func() { err = httpError(err) }()
	if ctx == nil {
		return nil, fmt.Errorf("http3: nil recovery context")
	}
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	if err := c.await(ctx); err != nil {
		return nil, err
	}
	// Serialize result publication before observing transport closure: failed
	// setup closes QUIC, but later callers must retain its original cause.
	// Acquiring the slot is itself cancelable.
	select {
	case c.recoveryLock <- struct{}{}:
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
	defer func() { <-c.recoveryLock }()
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	if c.replacement != nil || c.recoveryErr != nil {
		return c.replacement, c.recoveryErr
	}
	select {
	case <-c.conn.HandshakeComplete():
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case <-c.conn.Context().Done():
		return nil, context.Cause(c.conn.Context())
	}
	if c.conn.Context().Err() != nil {
		return nil, context.Cause(c.conn.Context())
	}
	if c.ctx.Err() != nil {
		return nil, context.Cause(c.ctx)
	}
	// A successfully used early generation needs no reconstruction. A connection
	// with no early attempt likewise retains its ordinary session.
	if c.conn.ConnectionState().Used0RTT {
		return c, nil
	}
	// Stream operations distinguish rejection from an ordinary non-early dial;
	// session cancellation carries that same outcome without allocating a stream.
	select {
	case <-c.session.Done():
	default:
		// WaitEncoder is already ready for early sessions, so wait for the transport
		// accept loop to publish rejection when an early generation was offered.
		if c.earlySession {
			select {
			case <-c.session.Done():
			case <-ctx.Done():
				return nil, context.Cause(ctx)
			}
		} else {
			return c, nil
		}
	}
	if _, err := c.session.WaitEncoder(context.Background()); !errors.Is(err, quic.Err0RTTRejected) {
		return nil, err
	}
	if err := c.retireRejected(); err != nil {
		return nil, err
	}
	// Ownership has transferred. Failure after this point is terminal and closes
	// QUIC; future calls return the same failure instead of duplicating streams.
	interrupted := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(interrupted); c.conn.CloseWithError(0x100, "HTTP3 recovery canceled") })
	_, err = c.conn.NextConnection(ctx)
	var fresh *ClientConn
	if err == nil {
		fresh, err = NewClientConn(c.lifetime, c.conn, "https://"+c.origin, &c.config)
	}
	if !stop() {
		<-interrupted
	}
	if ctx.Err() != nil {
		err = context.Cause(ctx)
	}
	if err != nil {
		if fresh != nil {
			fresh.Close()
		}
		c.conn.CloseWithError(0x100, "HTTP3 recovery failed")
		c.recoveryErr = err
		return nil, err
	}
	c.replacement = fresh
	return fresh, nil
}
