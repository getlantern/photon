package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/netip"

	"github.com/getlantern/photon/internal/transport"
)

// Dial establishes a connection using this Transport's socket. addr must be a
// resolved *net.UDPAddr. A blank TLS ServerName defaults to its IP address.
// Context values are retained by the connection and its streams; canceling ctx
// after success leaves the connection open. Failed attempts do not
// close the listener, other connections or the socket. Config.LocalAddr applies
// only to DialAddr; this method uses the already bound Transport.Conn.
func (t *Transport) Dial(ctx context.Context, addr net.Addr, tlsConf *tls.Config, conf *Config) (*Conn, error) {
	return t.dial(ctx, addr, tlsConf, conf, false)
}

// DialEarly permits replayable 0-RTT when a usable TLS session is cached. Rejected
// streams are not replayed automatically; see DialAddrEarly and NextConnection.
func (t *Transport) DialEarly(ctx context.Context, addr net.Addr, tlsConf *tls.Config, conf *Config) (*Conn, error) {
	return t.dial(ctx, addr, tlsConf, conf, true)
}

func (t *Transport) dial(ctx context.Context, addr net.Addr, tlsConf *tls.Config, conf *Config, early bool) (*Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cfg, params, err := prepareConfig(conf)
	if err != nil {
		return nil, err
	}
	cfg.earlyDial = early
	if err := validateTLS(tlsConf); err != nil {
		return nil, err
	}
	udp, ok := addr.(*net.UDPAddr)
	if !ok || udp == nil || udp.Port <= 0 || udp.Port > 65535 {
		return nil, errors.New("quic: peer must be a resolved UDP address with a nonzero port")
	}
	peer := udp.AddrPort()
	if !peer.IsValid() || peer.Addr().IsUnspecified() || peer.Addr().IsMulticast() {
		return nil, errors.New("quic: invalid peer IP address")
	}
	peer = netip.AddrPortFrom(peer.Addr().Unmap(), peer.Port())
	tlsCopy := tlsConf.Clone()
	if tlsCopy.ServerName == "" {
		tlsCopy.ServerName = peer.Addr().String()
	}
	runtime, err := t.init(cfg.MaxConnections)
	if err != nil {
		return nil, err
	}
	c, err := dialPrepared(ctx, peer, tlsCopy, cfg, params, runtime)
	if errors.Is(err, transport.ErrEndpointClosed) {
		return nil, ErrTransportClosed
	}
	return c, err
}
