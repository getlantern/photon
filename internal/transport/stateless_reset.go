package transport

import (
	"crypto/subtle"
	"net"
)

// StatelessResetError reports recognition of a stateless reset token.
// Temporary matches the reference API; recovery requires a new connection.
type StatelessResetError struct{}

func (*StatelessResetError) Error() string   { return "quic: peer stateless reset" }
func (*StatelessResetError) Unwrap() error   { return net.ErrClosed }
func (*StatelessResetError) Timeout() bool   { return false }
func (*StatelessResetError) Temporary() bool { return true }

var ErrStatelessReset = &StatelessResetError{}

// The socket owner must deliver otherwise-unroutable datagrams from this
// connection's peer address too. Reset datagrams carry unpredictable routing
// bytes. Tokens for unused or retired peer IDs must never match.
func (c *Conn) isStatelessReset(data []byte) bool {
	if len(data) < 21 {
		return false
	}
	suffix := data[len(data)-16:]
	match := 0
	check := func(ids *peerIDs) {
		if ids == nil {
			return
		}
		for _, id := range ids.active {
			if id.used && id.hasToken {
				match |= subtle.ConstantTimeCompare(suffix, id.token[:])
			}
		}
	}
	if c.multipath != nil {
		for id, state := range c.multipath.paths {
			if c.multipath.abandoned[id] == nil {
				check(state.peer)
			}
		}
	} else {
		check(c.peerIDs)
	}
	return match != 0
}
