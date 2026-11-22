package quic

import (
	"context"
	"errors"
	"net"
	"net/netip"

	"github.com/getlantern/photon/internal/transport"
)

// Path is a client path. AddPath creates an unvalidated migration handle using
// a caller-owned Transport; Close never closes that Transport's socket and cannot
// close the currently active migration path. OpenPath creates a validated
// multipath path with an owned socket; its Close abandons that path and socket.
// Closing the last viable multipath path is rejected. Multipath IDs cannot be reused.
type Path struct {
	migration     *transport.MigrationPath
	server        *transport.UDPServer
	conn          *transport.Conn
	id            uint64
	local, remote netip.AddrPort
}

func (p *Path) ID() uint64 { return p.id }
func (p *Path) LocalAddr() net.Addr {
	if p.migration != nil {
		local, _ := p.migration.Addresses()
		return net.UDPAddrFromAddrPort(local)
	}
	if p.conn != nil {
		if local, _, ok := p.conn.CurrentPathAddresses(p.id); ok {
			return net.UDPAddrFromAddrPort(local)
		}
	}
	return net.UDPAddrFromAddrPort(p.local)
}
func (p *Path) RemoteAddr() net.Addr {
	if p.migration != nil {
		_, remote := p.migration.Addresses()
		return net.UDPAddrFromAddrPort(remote)
	}
	if p.conn != nil {
		if _, remote, ok := p.conn.CurrentPathAddresses(p.id); ok {
			return net.UDPAddrFromAddrPort(remote)
		}
	}
	return net.UDPAddrFromAddrPort(p.remote)
}

// PathAddresses returns the last bound local and remote addresses for a
// negotiated multipath path. The pair is a consistent snapshot, retained after
// abandonment or shutdown. A found binding does not imply current reachability.
func (c *Conn) PathAddresses(id uint64) (local, remote net.Addr, ok bool) {
	if c.raw == nil {
		return nil, nil, false
	}
	l, r, ok := c.raw.CurrentPathAddresses(id)
	if !ok {
		return nil, nil, false
	}
	return net.UDPAddrFromAddrPort(l), net.UDPAddrFromAddrPort(r), true
}

// Migrate moves this path to a new concrete local socket, retaining its path ID.
// Port zero chooses a free port. The address family must remain unchanged.
// Completion and cancellation follow Conn.Migrate; LocalAddr reports the result.
func (p *Path) Migrate(ctx context.Context, local *net.UDPAddr) error {
	if p.migration != nil {
		return errors.New("quic: use Probe and Switch with an AddPath handle")
	}
	if local == nil || local.Port < 0 || local.Port > 65535 {
		return errors.New("quic: explicit migration address required")
	}
	return p.server.MigratePath(ctx, p.conn, p.id, local.AddrPort())
}

func (p *Path) Close() error {
	if p.migration != nil {
		return p.migration.Close()
	}
	return p.server.CloseCurrentOpenedPath(context.Background(), p.conn, p.id)
}

// OpenPath opens and validates an additional path on a dialed connection.
// Multipath must be negotiated. IDs 1–3 are supported, each usable once,
// in increasing initiation order. Concurrent calls are ordered by the owner;
// initiating a higher ID prevents later initiation of skipped lower IDs.
// Both addresses require concrete IPs; local port zero selects a free port.
// The peer must permit active migration to its handshake address.
// The context bounds CID exchange and validation. Cancellation closes the new
// socket; an ID whose probe was queued remains consumed. The peer can use its
// existing listener address. Servers can validate client NAT address rebinding;
// local socket migration is available through Conn.Migrate and Path.Migrate; address-family
// changes remain unsupported.
func (c *Conn) OpenPath(ctx context.Context, id uint64, local, remote *net.UDPAddr) (*Path, error) {
	if local == nil || remote == nil || local.Port < 0 || local.Port > 65535 || remote.Port < 1 || remote.Port > 65535 {
		return nil, errors.New("quic: explicit path addresses required")
	}
	address, err := c.server.OpenPath(ctx, c.raw, id, local.AddrPort(), remote.AddrPort())
	if err != nil {
		return nil, err
	}
	return &Path{server: c.server, conn: c.raw, id: id, local: address, remote: remote.AddrPort()}, nil
}

// AbandonPath retires one negotiated path, including primary path ID zero.
// Another validated path must remain. Queued and unacknowledged reliable data
// moves to that path. Repeated abandonment is harmless; IDs cannot be reused.
// This operation keeps UDP sockets open, including shared listener sockets.
// Use Path.Close to release a socket allocated by OpenPath.
func (c *Conn) AbandonPath(ctx context.Context, id uint64) error {
	return c.server.AbandonPath(ctx, c.raw, id)
}

// SetPathBackup asks the peer to use this path as backup (true) or available
// (false). This is a receive preference, not a change to local sending policy.
// It returns after queuing the update, not after peer acknowledgment. The peer
// may apply its own scheduling policy. Repeating the same preference is harmless.
func (c *Conn) SetPathBackup(ctx context.Context, id uint64, backup bool) error {
	return c.server.SetPathBackup(ctx, c.raw, id, backup)
}
