package quic

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestTransportVerifySourceAddress(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		for _, mode := range []string{"nil", "skip", "retry"} {
			t.Run(fmt.Sprintf("%v/%s", version, mode), func(t *testing.T) {
				ct, st := dialTLS(t)
				socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				defer socket.Close()
				policies := make(chan net.Addr, 8)
				infos := make(chan ClientInfo, 8)
				tr := &Transport{Conn: socket}
				if mode != "nil" {
					tr.VerifySourceAddress = func(addr net.Addr) bool { policies <- addr; return mode == "retry" }
				}
				defer tr.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				cfg := &Config{Versions: []Version{version}, GetConfigForClient: func(info *ClientInfo) (*Config, error) { infos <- *info; return nil, nil }}
				for generation := 0; generation < 2; generation++ {
					listener, err := tr.Listen(st, cfg)
					if err != nil {
						t.Fatal(err)
					}
					client, err := DialAddr(ctx, listener.Addr().String(), ct, &Config{Versions: []Version{version}})
					if err != nil {
						t.Fatal(err)
					}
					defer client.server.Close()
					accepted, err := listener.Accept(ctx)
					if err != nil {
						t.Fatal(err)
					}
					select {
					case info := <-infos:
						if info.AddrVerified != (mode == "retry") || info.RemoteAddr.(*net.UDPAddr).Port != client.LocalAddr().(*net.UDPAddr).Port {
							t.Fatal("validation metadata", info)
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					if mode != "nil" {
						select {
						case addr := <-policies:
							if addr.(*net.UDPAddr).Port != client.LocalAddr().(*net.UDPAddr).Port {
								t.Fatal("policy source", addr)
							}
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
					}
					// Multiple pre-Retry Initial packets can each invoke the policy.
					for len(policies) > 0 {
						addr := <-policies
						if addr.(*net.UDPAddr).Port != client.LocalAddr().(*net.UDPAddr).Port {
							t.Fatal("policy source", addr)
						}
					}
					select {
					case <-infos:
						t.Fatal("repeated config selection")
					default:
					}
					accepted.CloseWithError(0, "")
					if err := listener.Close(); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}
