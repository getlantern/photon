package quic

import (
	"context"
	"crypto/tls"
	"net"

	"github.com/getlantern/photon/internal/transport"
)

// Dial establishes one QUIC connection using a caller-owned UDP PacketConn.
// The socket must not be used by another QUIC listener, dialer or Transport while
// this connection is active. Use Transport for multiple connections on a socket.
// The socket remains open after failure or connection shutdown. On success its
// reader is released after QUIC closing/draining completes.
func Dial(ctx context.Context, c net.PacketConn, addr net.Addr, tlsConf *tls.Config, conf *Config) (*Conn, error) {
	return dialPacketConn(ctx, c, addr, tlsConf, conf, false)
}

// DialEarly is Dial with replayable 0-RTT enabled when a usable session is cached.
// See DialAddrEarly for rejection and handshake-completion semantics.
func DialEarly(ctx context.Context, c net.PacketConn, addr net.Addr, tlsConf *tls.Config, conf *Config) (*Conn, error) {
	return dialPacketConn(ctx, c, addr, tlsConf, conf, true)
}

func dialPacketConn(ctx context.Context, socket net.PacketConn, addr net.Addr, tlsConf *tls.Config, conf *Config, early bool) (*Conn, error) {
	t := &Transport{Conn: socket}
	c, err := t.dial(ctx, addr, tlsConf, conf, early)
	if err != nil {
		_ = t.Close()
		return nil, err
	}
	// Keep the reader alive for terminal close retransmission and draining, then
	// release it without closing the application's socket. Arm only after the
	// complete dial (including version fallback) has captured its result.
	if err := c.server.Do(context.Background(), func(e *transport.ServerEndpoint) error {
		e.CloseWhenIdle()
		return nil
	}); err != nil {
		_ = t.Close()
		return nil, c.connectionError(err)
	}
	return c, nil
}

// Listen accepts QUIC connections on a caller-owned UDP PacketConn. The socket
// must be exclusively available to this listener until Close returns. Closing
// this convenience listener terminates its accepted connections and releases
// its reader, but leaves the socket open. Use Transport.Listen to preserve
// accepted connections when replacing a listener or to share outgoing traffic.
func Listen(conn net.PacketConn, tlsConf *tls.Config, config *Config) (*Listener, error) {
	return listenPacketConn(conn, tlsConf, config, false)
}

// ListenEarly is Listen with acceptance before handshake completion. Applications
// must await HandshakeComplete before relying on client authentication.
func ListenEarly(conn net.PacketConn, tlsConf *tls.Config, config *Config) (*EarlyListener, error) {
	l, err := listenPacketConn(conn, tlsConf, config, true)
	if err != nil {
		return nil, err
	}
	return &EarlyListener{l}, nil
}

func listenPacketConn(socket net.PacketConn, tlsConf *tls.Config, config *Config, early bool) (*Listener, error) {
	t := &Transport{Conn: socket}
	l, err := t.listen(tlsConf, config, early)
	if err != nil {
		_ = t.Close()
		return nil, err
	}
	// This handle owns the runtime, though the runtime borrows the socket.
	l.shared = false
	return l, nil
}
