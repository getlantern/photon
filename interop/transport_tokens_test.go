package interop

import (
	"context"
	"fmt"
	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
	"net"
	"testing"
	"time"
)

func TestTransportTokenKeyRestartIndependent(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, mode := range []string{"random", "shared", "changed", "expired"} {
			t.Run(fmt.Sprintf("%v/%s", version, mode), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				defer socket.Close()
				key := quic.TokenGeneratorKey{7}
				tr := &quic.Transport{Conn: socket}
				if mode != "random" {
					tr.TokenGeneratorKey = &key
					if mode == "expired" {
						tr.MaxTokenAge = time.Nanosecond
					}
				}
				defer func() { tr.Close() }()
				cache := &observedTokenStore{TokenStore: upstream.NewLRUTokenStore(4, 4), stored: make(chan struct{}, 4)}
				infos := make(chan quic.ClientInfo, 4)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				for round := 0; round < 3; round++ {
					if round == 2 {
						if err := tr.Close(); err != nil {
							t.Fatal(err)
						}
						if mode == "changed" {
							key[0]++
						}
						tr = &quic.Transport{Conn: socket}
						if mode != "random" {
							tr.TokenGeneratorKey = &key
							if mode == "expired" {
								tr.MaxTokenAge = time.Nanosecond
							}
						}
					}
					l, err := tr.Listen(st, &quic.Config{Versions: []quic.Version{version}, GetConfigForClient: func(info *quic.ClientInfo) (*quic.Config, error) { infos <- *info; return nil, nil }})
					if err != nil {
						t.Fatal(err)
					}
					client, err := dialUpstreamAddr(t, ctx, l.Addr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, TokenStore: cache})
					if err != nil {
						t.Fatal(err)
					}
					defer client.CloseWithError(0, "")
					if _, err := l.Accept(ctx); err != nil {
						t.Fatal(err)
					}
					select {
					case info := <-infos:
						want := mode != "expired" && (round == 1 || round == 2 && mode == "shared")
						if info.AddrVerified != want {
							t.Fatalf("round %d verified %v want %v", round, info.AddrVerified, want)
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					select {
					case <-cache.stored:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					client.CloseWithError(0, "")
					if err := l.Close(); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}
