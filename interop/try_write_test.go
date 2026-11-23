package interop

import (
	"bytes"
	"context"
	"fmt"
	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
	"io"
	"testing"
	"time"
)

func TestTryWriteAllUpstream(t *testing.T) {
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
						TryWriteAll([]byte) error
						SetWriteDeadline(time.Time) error
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
					tx.SetWriteDeadline(time.Now().Add(-time.Second))
					payload := bytes.Repeat([]byte("atomic payload"), 400)
					want := bytes.Clone(payload)
					if err := tx.TryWriteAll(payload); err != nil {
						t.Fatal("expired deadline affected try", err)
					}
					clear(payload)
					tx.SetWriteDeadline(time.Now().Add(time.Second))
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
					if err != nil || !bytes.Equal(got, want) {
						t.Fatal("whole owned payload", len(got), err)
					}
				})
			}
		}
	}
}
