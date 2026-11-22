package interop

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

func TestPublicPartialResetSenderUpstream(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, server := range []bool{false, true} {
			for _, support := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/server=%v/support=%v", v, server, support), func(t *testing.T) {
					ct, st := tlsConfigs(t)
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					var own *quic.Conn
					var peer *upstream.Conn
					var err error
					if server {
						l, err := quic.ListenAddr("127.0.0.1:0", st, &quic.Config{Versions: []quic.Version{v}})
						if err != nil {
							t.Fatal(err)
						}
						defer l.Close()
						peer, err = dialUpstreamAddr(t, ctx, l.Addr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}, EnableStreamResetPartialDelivery: support})
						if err != nil {
							t.Fatal(err)
						}
						own, err = l.Accept(ctx)
					} else {
						l, err := upstream.ListenAddr("127.0.0.1:0", st, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}, EnableStreamResetPartialDelivery: support})
						if err != nil {
							t.Fatal(err)
						}
						defer l.Close()
						own, err = quic.DialAddr(ctx, l.Addr().String(), ct, &quic.Config{Versions: []quic.Version{v}})
						if err != nil {
							t.Fatal(err)
						}
						peer, err = l.Accept(ctx)
					}
					if err != nil {
						t.Fatal(err)
					}
					defer own.CloseWithError(0, "")
					defer peer.CloseWithError(0, "")
					tx, err := own.OpenUniStreamSync(ctx)
					if err != nil {
						t.Fatal(err)
					}
					prefix := bytes.Repeat([]byte("reliable"), 400)
					for _, part := range [][]byte{prefix[:1600], prefix[1600:]} {
						if n, err := tx.Write(part); err != nil || n != len(part) {
							t.Fatal(n, err)
						}
						tx.SetReliableBoundary()
					}
					suffix := bytes.Repeat([]byte("optional"), 100)
					if _, err := tx.Write(suffix); err != nil {
						t.Fatal(err)
					}
					tx.CancelWrite(7)
					tx.SetReliableBoundary() // Must not increase the frozen reset boundary.
					tx.CancelWrite(99)
					if _, err := tx.Write([]byte("late")); err == nil {
						t.Fatal("write after reset")
					}
					rx, err := peer.AcceptUniStream(ctx)
					if err != nil {
						t.Fatal(err)
					}
					deadline, _ := ctx.Deadline()
					rx.SetReadDeadline(deadline)
					got, err := io.ReadAll(rx)
					var reset *upstream.StreamError
					if !errors.As(err, &reset) || reset.ErrorCode != 7 || !reset.Remote {
						t.Fatal("reset", err)
					}
					// Bytes already delivered before reset may include some optional suffix.
					// The negotiated prefix must be delivered completely in every case.
					if support && !bytes.HasPrefix(got, prefix) {
						t.Fatal("missing reliable prefix", len(got))
					}
					if len(got) > len(prefix)+len(suffix) || !bytes.Equal(got, append(bytes.Clone(prefix), suffix...)[:len(got)]) {
						t.Fatal("corrupt delivered prefix")
					}
					if err := tx.Release(ctx); err != nil {
						t.Fatal("release", err)
					}
					var ownReset *quic.StreamError
					if err := tx.WaitAcknowledged(ctx); !errors.As(err, &ownReset) || ownReset.ErrorCode != 7 || ownReset.Remote {
						t.Fatal("delivery result", err)
					}
				})
			}
		}
	}
}
