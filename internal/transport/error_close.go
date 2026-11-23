package transport

import (
	"errors"
	"time"
)

// Record the same transport failure that selects the wire close code, even when
// no usable protection keys remain. Remote closes and unrelated failures do not
// cause a response. TLS diagnostics remain local.
func (c *Conn) closeReceiveFailure(err error, now time.Time) error {
	if err == nil || c.closed || c.Closing() || c.Draining() {
		return err
	}
	normalized := connectionFailure(err)
	var failure *ConnectionTransportError
	if !errors.As(normalized, &failure) || failure.Remote {
		return err
	}
	terminal := c.fail(normalized)
	if !errors.As(terminal, &failure) || failure.Remote {
		return terminal
	}
	reason := failure.ErrorMessage
	if failure.ErrorCode.IsCryptoError() {
		reason = ""
	}
	_ = c.CloseTransportError(uint64(failure.ErrorCode), failure.FrameType, reason, now)
	return terminal
}
