package interop

import (
	"context"
	"crypto/tls"
	"net"
	"testing"

	upstream "github.com/quic-go/quic-go"
)

// dialUpstreamAddr preserves upstream's single-use Dial transport, but binds
// the socket to the destination's own IP. On Darwin, a socket on a wildcard
// address can share a port with an unrelated listener on a specific address,
// which then receives the replies instead (see loopbackDial). See
// docs/diagnostics for an observed failure and OS probe.
func dialUpstreamAddr(t testing.TB, ctx context.Context, address string, tlsConfig *tls.Config, config *upstream.Config) (*upstream.Conn, error) {
	t.Helper()
	remote, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return nil, err
	}
	network := "udp6"
	if remote.IP.To4() != nil {
		network = "udp4"
	}
	// Bind to the server's own IP, not the unspecified address.
	socket, err := net.ListenUDP(network, &net.UDPAddr{IP: remote.IP})
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { socket.Close() })
	// DialAddr infers TLS ServerName from the textual host; Dial otherwise uses
	// the resolved IP. Preserve the original behavior without mutating the caller.
	if tlsConfig != nil && tlsConfig.ServerName == "" {
		tlsConfig = tlsConfig.Clone()
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			socket.Close()
			return nil, err
		}
		tlsConfig.ServerName = host
	}
	conn, err := upstream.Dial(ctx, socket, remote, tlsConfig, config)
	if err != nil {
		socket.Close()
	}
	return conn, err
}

// loopbackDial is a quic-go http3.Transport Dial that binds its socket to the
// server's own address rather than the unspecified one. On macOS a socket on
// the unspecified address can be given the same port as a server bound to
// 127.0.0.1; the server then sees its own address as the peer, its replies
// loop back to itself, and the handshake times out.
func loopbackDial(t testing.TB) func(context.Context, string, *tls.Config, *upstream.Config) (*upstream.Conn, error) {
	return func(ctx context.Context, addr string, tlsConfig *tls.Config, config *upstream.Config) (*upstream.Conn, error) {
		remote, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			return nil, err
		}
		socket, err := net.ListenUDP("udp", &net.UDPAddr{IP: remote.IP})
		if err != nil {
			return nil, err
		}
		t.Cleanup(func() { socket.Close() })
		conn, err := upstream.DialEarly(ctx, socket, remote, tlsConfig, config)
		if err != nil {
			socket.Close()
		}
		return conn, err
	}
}
