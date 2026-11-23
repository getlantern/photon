package transport

import "net"

// IdleTimeoutError reports expiration of connection or handshake inactivity.
// The connection is closed; retrying an operation on it cannot succeed.
type IdleTimeoutError struct{}

func (*IdleTimeoutError) Error() string   { return "quic: idle timeout" }
func (*IdleTimeoutError) Timeout() bool   { return true }
func (*IdleTimeoutError) Temporary() bool { return false }
func (*IdleTimeoutError) Unwrap() error   { return net.ErrClosed }

// HandshakeTimeoutError reports expiration of the absolute handshake limit.
type HandshakeTimeoutError struct{}

func (*HandshakeTimeoutError) Error() string   { return "quic: handshake timeout" }
func (*HandshakeTimeoutError) Timeout() bool   { return true }
func (*HandshakeTimeoutError) Temporary() bool { return false }
func (*HandshakeTimeoutError) Unwrap() error   { return net.ErrClosed }

var (
	ErrIdleTimeout                = &IdleTimeoutError{}
	ErrHandshakeTimeout           = &HandshakeTimeoutError{}
	_                   net.Error = (*IdleTimeoutError)(nil)
	_                   net.Error = (*HandshakeTimeoutError)(nil)
)
