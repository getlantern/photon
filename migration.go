package quic

import (
	"context"
	"errors"
	"net"
)

// Migrate moves the primary path of a dialed connection to a new local UDP
// socket, waiting for peer connection IDs and validation. The IP must be concrete and use the existing
// address family; port zero selects a free port. The original connection must
// have a concrete LocalAddr and nonzero connection IDs, and the peer must allow
// active migration. Multipath negotiation is not required.
// A pending migration on this path, including automatic preferred-address
// validation, makes this call return an error without replacing that attempt.
// Cancellation cancels an uncommitted attempt and closes its socket. If commit
// wins the race, Migrate returns success. An unused previous socket is closed;
// sockets still backing other paths remain open.
func (c *Conn) Migrate(ctx context.Context, local *net.UDPAddr) error {
	if local == nil || local.Port < 0 || local.Port > 65535 {
		return errors.New("quic: explicit migration address required")
	}
	return c.server.Migrate(ctx, c.raw, local.AddrPort())
}
