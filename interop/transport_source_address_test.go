package interop

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

func TestTransportVerifySourceAddressIndependent(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, mode := range []string{"nil", "skip", "retry"} {
			t.Run(fmt.Sprintf("%v/%s", version, mode), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				defer socket.Close()
				policies := make(chan net.Addr, 8)
				infos := make(chan quic.ClientInfo, 8)
				tr := &quic.Transport{Conn: socket}
				if mode != "nil" {
					tr.VerifySourceAddress = func(addr net.Addr) bool { policies <- addr; return mode == "retry" }
				}
				defer tr.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				cfg := &quic.Config{Versions: []quic.Version{version}, GetConfigForClient: func(info *quic.ClientInfo) (*quic.Config, error) { infos <- *info; return nil, nil }}
				for generation := 0; generation < 2; generation++ {
					listener, err := tr.Listen(st, cfg)
					if err != nil {
						t.Fatal(err)
					}
					client, err := dialUpstreamAddr(t, ctx, listener.Addr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}})
					if err != nil {
						t.Fatal(err)
					}
					defer client.CloseWithError(0, "")
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
