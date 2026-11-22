package interop

import (
	"context"
	"errors"
	"fmt"
	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
	"io"
	"testing"
	"time"
)

func TestWriteWithLimitUpstream(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, serverRole := range []bool{false, true} {
			for _, uni := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/server=%t/uni=%t", version, serverRole, uni), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					ct, st := tlsConfigs(t)
					var own *quic.Conn
					var peer *upstream.Conn
					peerConfig := &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, EnableStreamResetPartialDelivery: true}
					ownConfig := &quic.Config{Versions: []quic.Version{version}, EnableStreamResetPartialDelivery: true}
					if serverRole {
						l, err := quic.ListenAddr("127.0.0.1:0", st, ownConfig)
						if err != nil {
							t.Fatal(err)
						}
						defer l.Close()
						peer, err = dialUpstreamAddr(t, ctx, l.Addr().String(), ct, peerConfig)
						if err != nil {
							t.Fatal(err)
						}
						defer peer.CloseWithError(0, "")
						own, err = l.Accept(ctx)
						if err != nil {
							t.Fatal(err)
						}
						defer own.CloseWithError(0, "")
					} else {
						l, err := upstream.ListenAddr("127.0.0.1:0", st, peerConfig)
						if err != nil {
							t.Fatal(err)
						}
						defer l.Close()
						own, err = quic.DialAddr(ctx, l.Addr().String(), ct, ownConfig)
						if err != nil {
							t.Fatal(err)
						}
						defer own.CloseWithError(0, "")
						peer, err = l.Accept(ctx)
						if err != nil {
							t.Fatal(err)
						}
						defer peer.CloseWithError(0, "")
					}

					type sender interface {
						WriteWithLimit([]byte, func(int) int) (int, error)
						Close() error
					}
					var tx sender
					var err error
					if uni {
						tx, err = own.OpenUniStream()
					} else {
						tx, err = own.OpenStream()
					}
					if err != nil {
						t.Fatal(err)
					}
					calls := 0
					if n, err := tx.WriteWithLimit([]byte("abcdef"), func(maximum int) int { calls++; return 0 }); n != 0 || !errors.Is(err, quic.ErrWriteLimitReached) {
						t.Fatal("zero allowance", n, err)
					}
					offered := 0
					n, err := tx.WriteWithLimit([]byte("abcdef"), func(maximum int) int { calls++; offered = maximum; return 2 })
					if n != 2 || !errors.Is(err, quic.ErrWriteLimitReached) || offered != 6 {
						t.Fatal("limited prefix", n, err, offered)
					}
					if n, err := tx.WriteWithLimit([]byte("cdef"), func(maximum int) int { calls++; return maximum + 100 }); n != 4 || err != nil {
						t.Fatal("suffix retry", n, err)
					}
					if calls != 3 {
						t.Fatal("callback count", calls)
					}
					if err := tx.Close(); err != nil {
						t.Fatal(err)
					}
					var rx interface {
						Read([]byte) (int, error)
						SetReadDeadline(time.Time) error
					}
					if uni {
						rx, err = peer.AcceptUniStream(ctx)
					} else {
						rx, err = peer.AcceptStream(ctx)
					}
					if err != nil {
						t.Fatal(err)
					}
					rx.SetReadDeadline(time.Now().Add(time.Second))
					got, err := io.ReadAll(rx)
					if err != nil || string(got) != "abcdef" {
						t.Fatal("exact committed bytes", string(got), err)
					}
				})
			}
		}
	}
}
