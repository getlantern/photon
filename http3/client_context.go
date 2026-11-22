package http3

import (
	"context"

	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/wire"
)

// Context returns the underlying QUIC connection's context. It is canceled on
// connection closure and retains the QUIC close cause. Replacing a rejected
// early HTTP session does not replace or cancel this transport context.
func (c *ClientConn) Context() context.Context { return c.conn.Context() }

// CloseWithError initiates connection closure with an application code and
// message. Like Close, a retired early session cannot close its replacement.
// Use Close to join this HTTP session's workers after initiating closure.
func (c *ClientConn) CloseWithError(code quic.ApplicationErrorCode, msg string) error {
	// Validate before consuming ownership: a rejected close leaves the live
	// connection usable and still permits a subsequent valid close.
	if uint64(code) > wire.MaxVarint {
		return wire.ErrVarintRange
	}
	return c.ownership.abort(uint64(code), msg)
}
