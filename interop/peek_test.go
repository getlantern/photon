package interop

import (
	"context"
	"errors"
	"fmt"
	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
	"io"
	"os"
	"testing"
	"time"
)

func TestPeekUpstream(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, serverRole := range []bool{false, true} {
			for _, uni := range []bool{false, true} {
				for _, partial := range []bool{false, true} {
					t.Run(fmt.Sprintf("%x/server=%t/uni=%t/partial=%t", version, serverRole, uni, partial), func(t *testing.T) {
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
							Write([]byte) (int, error)
							Close() error
							CancelWrite(upstream.StreamErrorCode)
							SetReliableBoundary()
						}
						type receiver interface {
							Read([]byte) (int, error)
							Peek([]byte) (int, error)
							SetReadDeadline(time.Time) error
							SetReceiveFinalSizeCallback(func(int64))
						}
						var tx sender
						var rx receiver
						var err error
						if uni {
							tx, err = peer.OpenUniStream()
						} else {
							tx, err = peer.OpenStream()
						}
						if err != nil {
							t.Fatal(err)
						}
						if n, err := tx.Write([]byte("ab")); n != 2 || err != nil {
							t.Fatal(n, err)
						}
						if uni {
							rx, err = own.AcceptUniStream(ctx)
						} else {
							rx, err = own.AcceptStream(ctx)
						}
						if err != nil {
							t.Fatal(err)
						}
						rx.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
						b := []byte("xxxxx")
						if n, err := rx.Peek(b[:3]); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) || string(b) != "xxxxx" {
							t.Fatal("incomplete peek", n, err, string(b))
						}
						rx.SetReadDeadline(time.Now().Add(time.Second))
						finals := make(chan int64, 2)
						rx.SetReceiveFinalSizeCallback(func(size int64) { finals <- size })
						if n, err := tx.Write([]byte("cd")); n != 2 || err != nil {
							t.Fatal(n, err)
						}
						if partial {
							tx.SetReliableBoundary()
							tx.CancelWrite(77)
						} else if err := tx.Close(); err != nil {
							t.Fatal(err)
						}
						for i := 0; i < 2; i++ {
							if n, err := rx.Peek(b[:4]); n != 4 || err != nil || string(b[:4]) != "abcd" {
								t.Fatal("exact peek", n, err, string(b))
							}
							n, err := rx.Peek(b)
							if n != 4 || string(b[:4]) != "abcd" {
								t.Fatal("terminal prefix", n, err, string(b))
							}
							if partial {
								var reset *quic.StreamError
								if !errors.As(err, &reset) || reset.ErrorCode != 77 || !reset.Remote {
									t.Fatal("partial reset", err)
								}
							} else if !errors.Is(err, io.EOF) {
								t.Fatal("FIN", err)
							}
						}
						got, err := io.ReadAll(rx)
						select {
						case size := <-finals:
							if size != 4 {
								t.Fatal("independent final-size callback", size)
							}
						case <-ctx.Done():
							t.Fatal("missing final-size callback", ctx.Err())
						}
						select {
						case size := <-finals:
							t.Fatal("duplicate final-size callback", size)
						default:
						}
						if string(got) != "abcd" {
							t.Fatal("peek consumed data", string(got), err)
						}
						if partial {
							var reset *quic.StreamError
							if !errors.As(err, &reset) || reset.ErrorCode != 77 {
								t.Fatal("read reset", err)
							}
						} else if err != nil {
							t.Fatal(err)
						}
					})
				}
			}
		}
	}
}
