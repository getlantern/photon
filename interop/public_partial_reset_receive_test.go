package interop

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
	"io"
	"testing"
	"time"
)

func TestPublicPartialResetReceiverUpstream(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, server := range []bool{false, true} {
			for _, uni := range []bool{false, true} {
				for _, enabled := range []bool{false, true} {
					t.Run(fmt.Sprintf("%x/server=%v/uni=%v/enabled=%v", v, server, uni, enabled), func(t *testing.T) {
						ct, st := tlsConfigs(t)
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						var own *quic.Conn
						var peer *upstream.Conn
						var err error
						if server {
							// Both enabling and disabling follow selected connection policy.
							l, err := quic.ListenAddr("127.0.0.1:0", st, &quic.Config{Versions: []quic.Version{v}, EnableStreamResetPartialDelivery: !enabled, GetConfigForClient: func(*quic.ClientInfo) (*quic.Config, error) {
								return &quic.Config{EnableStreamResetPartialDelivery: enabled}, nil
							}})
							if err != nil {
								t.Fatal(err)
							}
							defer l.Close()
							peer, err = dialUpstreamAddr(t, ctx, l.Addr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}})
							if err != nil {
								t.Fatal(err)
							}
							own, err = l.Accept(ctx)
						} else {
							l, err := upstream.ListenAddr("127.0.0.1:0", st, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}})
							if err != nil {
								t.Fatal(err)
							}
							defer l.Close()
							own, err = quic.DialAddr(ctx, l.Addr().String(), ct, &quic.Config{Versions: []quic.Version{v}, EnableStreamResetPartialDelivery: enabled})
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
						support := own.ConnectionState().SupportsStreamResetPartialDelivery
						peerSupport := peer.ConnectionState().SupportsStreamResetPartialDelivery
						if support.Local != enabled || support.Remote || peerSupport.Remote != enabled || peerSupport.Local {
							t.Fatal("asymmetric negotiated state", support, peerSupport)
						}
						var tx interface {
							Write([]byte) (int, error)
							SetReliableBoundary()
							CancelWrite(upstream.StreamErrorCode)
						}
						if uni {
							tx, err = peer.OpenUniStreamSync(ctx)
						} else {
							tx, err = peer.OpenStreamSync(ctx)
						}
						if err != nil {
							t.Fatal(err)
						}
						prefix := bytes.Repeat([]byte("reliable"), 400)
						if n, err := tx.Write(prefix); err != nil || n != len(prefix) {
							t.Fatal(n, err)
						}
						tx.SetReliableBoundary()
						suffix := bytes.Repeat([]byte("optional"), 100)
						if _, err := tx.Write(suffix); err != nil {
							t.Fatal(err)
						}
						tx.CancelWrite(7)
						var rx interface {
							Read([]byte) (int, error)
							SetReadDeadline(time.Time) error
							Release(context.Context) error
						}
						if uni {
							rx, err = own.AcceptUniStream(ctx)
						} else {
							var bidi *quic.Stream
							bidi, err = own.AcceptStream(ctx)
							if err == nil {
								bidi.CancelWrite(0)
								rx = bidi
							}
						}
						if err != nil {
							t.Fatal(err)
						}
						deadline, _ := ctx.Deadline()
						rx.SetReadDeadline(deadline)
						got, err := io.ReadAll(rx)
						var reset *quic.StreamError
						if !errors.As(err, &reset) || reset.ErrorCode != 7 || !reset.Remote {
							t.Fatal("reset", err)
						}
						if enabled && !bytes.HasPrefix(got, prefix) {
							t.Fatal("missing reliable prefix", len(got))
						}
						all := append(bytes.Clone(prefix), suffix...)
						if len(got) > len(all) || !bytes.Equal(got, all[:len(got)]) {
							t.Fatal("corrupt bytes")
						}
						if err := rx.Release(ctx); err != nil {
							t.Fatal("receive retirement", err)
						}
						own.CloseWithError(0, "")
						if own.ConnectionState().SupportsStreamResetPartialDelivery != support {
							t.Fatal("lost capability snapshot")
						}
					})
				}
			}
		}
	}
}
