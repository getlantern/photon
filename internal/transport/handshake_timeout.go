package transport

import (
	"time"
)

// HandshakeDeadline selects the earlier local absolute or handshake idle limit,
// separate from the negotiated idle timeout and its PTO floor. Both start at the
// first timestamped operation. Activity can move only the idle limit, which ends
// at TLS completion together with the absolute limit. Adapters
// must service NextDatagram immediately after Start to arm these limits.
func (c *Conn) HandshakeDeadline() time.Time {
	if c.complete || c.err != nil || c.closed || c.handshakeStarted.IsZero() {
		return time.Time{}
	}
	deadline := c.handshakeStarted.Add(c.config.HandshakeTimeout)
	if c.config.HandshakeIdleTimeout > 0 {
		start := c.handshakeStarted
		if c.idle.last.After(start) {
			start = c.idle.last
		}
		if idle := start.Add(c.config.HandshakeIdleTimeout); idle.Before(deadline) {
			deadline = idle
		}
	}
	return deadline
}
func (c *Conn) checkHandshakeTimeout(now time.Time) error {
	if c.complete {
		return nil
	}
	if c.handshakeStarted.IsZero() {
		c.handshakeStarted = now
	}
	if !now.Before(c.HandshakeDeadline()) {
		var cause error = ErrHandshakeTimeout
		// Absolute expiry takes precedence if both limits have passed.
		if now.Before(c.handshakeStarted.Add(c.config.HandshakeTimeout)) {
			cause = ErrIdleTimeout
		}
		err := c.fail(cause)
		c.Close()
		return err
	}
	return nil
}
