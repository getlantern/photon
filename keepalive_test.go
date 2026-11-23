package quic

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func TestPublicKeepAlive(t *testing.T) {
	for _, v := range []Version{Version1, Version2} {
		for _, serverKeeps := range []bool{false, true} {
			for _, multipath := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/server=%t/multipath=%t", v, serverKeeps, multipath), func(t *testing.T) {
					t.Parallel()
					clientTLS, serverTLS := dialTLS(t)
					clientCfg := &Config{Versions: []Version{v}, MaxIdleTimeout: time.Second, DisablePathMTUDiscovery: true}
					serverCfg := &Config{Versions: []Version{v}, MaxIdleTimeout: time.Second, DisablePathMTUDiscovery: true}
					if multipath {
						clientCfg.EnableMultipath = true
						serverCfg.EnableMultipath = true
						clientCfg.LocalAddr = &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}
					}
					if serverKeeps {
						serverCfg.KeepAlivePeriod = 100 * time.Millisecond
					} else {
						clientCfg.KeepAlivePeriod = 100 * time.Millisecond
					}
					l, err := ListenAddr("127.0.0.1:0", serverTLS, serverCfg)
					if err != nil {
						t.Fatal(err)
					}
					defer l.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 7*time.Second)
					defer cancel()
					c, err := DialAddr(ctx, l.Addr().String(), clientTLS, clientCfg)
					if err != nil {
						t.Fatal(err)
					}
					defer c.server.Close()
					peer, err := l.Accept(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if multipath {
						if !c.ConnectionState().SupportsMultipath || !peer.ConnectionState().SupportsMultipath {
							t.Fatal("multipath not negotiated")
						}
						path, err := c.OpenPath(ctx, 1, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}, l.Addr().(*net.UDPAddr))
						if err != nil {
							t.Fatal(err)
						}
						if path.ID() != 1 || path.LocalAddr().String() == c.LocalAddr().String() {
							t.Fatal("no distinct path socket")
						}
						if err := c.AbandonPath(ctx, 0); err != nil {
							t.Fatal(err)
						}
					}
					// Exceed the advertised timeout and initial 3-PTO floor without app data.
					timer := time.NewTimer(3500 * time.Millisecond)
					defer timer.Stop()
					select {
					case <-c.Context().Done():
						t.Fatal("client expired", context.Cause(c.Context()))
					case <-peer.Context().Done():
						t.Fatal("server expired", context.Cause(peer.Context()))
					case <-timer.C:
					}
					for _, endpoints := range [][2]*Conn{{c, peer}, {peer, c}} {
						tx, err := endpoints[0].OpenUniStreamSync(ctx)
						if err != nil {
							t.Fatal(err)
						}
						tx.SetWriteDeadline(time.Now().Add(time.Second))
						if _, err := tx.Write([]byte("alive")); err != nil {
							t.Fatal(err)
						}
						if err := tx.Close(); err != nil {
							t.Fatal(err)
						}
						rx, err := endpoints[1].AcceptUniStream(ctx)
						if err != nil {
							t.Fatal(err)
						}
						rx.SetReadDeadline(time.Now().Add(time.Second))
						if b, err := io.ReadAll(rx); err != nil || string(b) != "alive" {
							t.Fatalf("%q: %v", b, err)
						}
					}
				})
			}
		}
	}
}
