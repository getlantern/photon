package transport

import "errors"

// WatchHandshakeComplete registers one owner callback, or invokes it immediately
// when TLS has already completed. Failure does not report success. The callback
// must not block or reenter the engine; it may publish an immutable state snapshot.
func (c *Conn) WatchHandshakeComplete(notify func()) error {
	if err := c.check(); err != nil {
		return err
	}
	if notify == nil || c.onHandshakeComplete != nil {
		return errors.New("quic: invalid handshake completion observer")
	}
	if c.complete {
		notify()
	} else {
		c.onHandshakeComplete = notify
	}
	return nil
}

func (c *Conn) EarlyDataAccepted() bool { return c.tls.EarlyDataAccepted() }

// WatchHandshakeConfirmed follows the same owner contract as completion, but
// waits for HANDSHAKE_DONE at clients. Servers confirm when TLS completes.
func (c *Conn) WatchHandshakeConfirmed(notify func()) error {
	if err := c.check(); err != nil {
		return err
	}
	if notify == nil || c.onHandshakeConfirmed != nil {
		return errors.New("quic: invalid handshake confirmation observer")
	}
	if c.confirmed {
		notify()
	} else {
		c.onHandshakeConfirmed = notify
	}
	return nil
}

func (c *Conn) notifyHandshakeProgress() {
	if c.complete && c.onHandshakeComplete != nil {
		notify := c.onHandshakeComplete
		c.onHandshakeComplete = nil
		notify()
	}
	if c.confirmed && c.onHandshakeConfirmed != nil {
		notify := c.onHandshakeConfirmed
		c.onHandshakeConfirmed = nil
		notify()
	}
}
