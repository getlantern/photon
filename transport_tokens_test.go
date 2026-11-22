package quic

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestTransportTokenKeyDerivation(t *testing.T) {
	key := TokenGeneratorKey{9}
	retry, address, err := newTransportTokens(&key, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, same, _ := newTransportTokens(&key, 0)
	key[0]++
	_, changed, _ := newTransportTokens(&key, 0)
	peer := netip.MustParseAddrPort("192.0.2.1:1234")
	now := time.Unix(100, 0)
	opaque, err := address.Issue(peer, wire.Version1, now)
	if err != nil {
		t.Fatal(err)
	}
	if same.Validate(opaque, peer, wire.Version1, now) != nil || changed.Validate(opaque, peer, wire.Version1, now) == nil {
		t.Fatal("key binding")
	}
	if _, err := retry.Validate(opaque, peer, wire.Version1, []byte("retrycid"), now); err == nil {
		t.Fatal("token domains overlap")
	}
}

func TestTransportTokenKeyRestart(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		for _, mode := range []string{"random", "shared", "changed", "expired"} {
			t.Run(fmt.Sprintf("%v/%s", version, mode), func(t *testing.T) {
				ct, st := dialTLS(t)
				socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				defer socket.Close()
				key := TokenGeneratorKey{7}
				tr := &Transport{Conn: socket}
				if mode != "random" {
					tr.TokenGeneratorKey = &key
					if mode == "expired" {
						tr.MaxTokenAge = time.Nanosecond
					}
				}
				defer func() { tr.Close() }()
				cache := &signalingTokenStore{TokenStore: NewLRUTokenStore(4, 4), stored: make(chan struct{}, 4), popped: make(chan bool, 4)}
				infos := make(chan ClientInfo, 4)
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
						tr = &Transport{Conn: socket}
						if mode != "random" {
							tr.TokenGeneratorKey = &key
							if mode == "expired" {
								tr.MaxTokenAge = time.Nanosecond
							}
						}
					}
					l, err := tr.Listen(st, &Config{Versions: []Version{version}, GetConfigForClient: func(info *ClientInfo) (*Config, error) { infos <- *info; return nil, nil }})
					if err != nil {
						t.Fatal(err)
					}
					client, err := DialAddr(ctx, l.Addr().String(), ct, &Config{Versions: []Version{version}, TokenStore: cache})
					if err != nil {
						t.Fatal(err)
					}
					defer client.server.Close()
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
					if used := <-cache.popped; used != (round > 0) {
						t.Fatal("cached evidence not offered")
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

func TestTransportMaxTokenAge(t *testing.T) {
	peer := netip.MustParseAddrPort("192.0.2.1:1234")
	now := time.Unix(10000, 123456789)
	for _, age := range []time.Duration{0, 1, 500 * time.Millisecond, 30 * 24 * time.Hour} {
		_, tokens, err := newTransportTokens(nil, age)
		if err != nil {
			t.Fatal(err)
		}
		b, err := tokens.Issue(peer, Version1, now)
		if err != nil {
			t.Fatal(err)
		}
		want := age
		if want == 0 {
			want = 24 * time.Hour
		}
		if tokens.Validate(b, peer, Version1, now.Add(want-1)) != nil || tokens.Validate(b, peer, Version1, now.Add(want)) == nil {
			t.Fatal("configured expiry", age)
		}
	}
	tr := &Transport{MaxTokenAge: -1}
	if _, err := tr.init(1); err == nil || tr.runtime != nil {
		t.Fatal("negative age started transport")
	}
}
