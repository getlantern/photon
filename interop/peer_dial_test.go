package interop

import (
	"context"
	"crypto/tls"
	"net"
	"testing"

	upstream "github.com/quic-go/quic-go"
)

// dialUpstreamAddr preserves upstream's single-use Dial transport, but binds
// the socket in the destination's address family. On Darwin, DialAddr's IPv6
// wildcard can share a port with an unrelated IPv4 listener that receives the
// replies instead. See docs/diagnostics for the observed failure and OS probe.
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
	socket, err := net.ListenUDP(network, nil)
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
