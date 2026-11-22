package quic

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestConcreteOrdinaryClientPathOwnership(t *testing.T) {
	for _, host := range []string{"127.0.0.1", "::1"} {
		t.Run(host, func(t *testing.T) {
			clientTLS, serverTLS := dialTLS(t)
			clientTLS.ServerName = "127.0.0.1" // Fixture certificate identity; transport may use IPv6.
			listener, err := ListenAddr(net.JoinHostPort(host, "0"), serverTLS, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := DialAddr(ctx, listener.Addr().String(), clientTLS, &Config{LocalAddr: &net.UDPAddr{IP: net.ParseIP(host)}})
			if err != nil {
				t.Fatal(err)
			}
			defer client.CloseWithError(0, "")
			server, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, conn := range []*Conn{client, server} {
				local, remote, ok := conn.PathAddresses(0)
				if !ok || local.String() != conn.LocalAddr().String() || remote.String() != conn.RemoteAddr().String() || conn.ConnectionState().SupportsMultipath {
					t.Fatal("ordinary path ownership not established")
				}
				if _, _, ok := conn.PathAddresses(1); ok {
					t.Fatal("ordinary connection exposed extra path")
				}
				if err := conn.SetPathBackup(ctx, 0, true); err == nil {
					t.Fatal("ordinary connection advertised multipath status")
				}
			}
		})
	}
}
