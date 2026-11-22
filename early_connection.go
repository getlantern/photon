package quic

import (
	"context"
	"errors"

	"github.com/getlantern/photon/internal/transport"
)

// NextConnection waits for successful TLS completion and enables fresh stream
// opens and accepts after 0-RTT rejection. It returns the same connection and is
// safe to call concurrently or repeatedly. Rejected stream handles remain
// invalid, and applications must explicitly decide whether to resend their data.
// Cancellation before completion leaves the stream rejection gate in place.
func (c *Conn) NextConnection(ctx context.Context) (*Conn, error) {
	err := c.server.Wait(ctx, func(*transport.ServerEndpoint) (bool, error) {
		if err := c.raw.Err(); err != nil {
			return true, err
		}
		if !c.raw.HandshakeComplete() {
			return false, nil
		}
		c.earlyStreamsRecovered = true
		return true, nil
	})
	if err != nil {
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			return nil, context.Cause(ctx)
		}
		if errors.Is(err, transport.ErrEndpointClosed) && c.ctx.Err() != nil {
			return nil, context.Cause(c.ctx)
		}
		return nil, err
	}
	return c, nil
}
