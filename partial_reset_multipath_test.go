package quic

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

// Both extensions are negotiated over TLS; path one is validated through actual
// UDP traffic before the client abandons the primary path. This is own-peer
// integration coverage, not independent multipath interoperability evidence.
func TestPublicPartialResetMultipath(t *testing.T) {
	for _, v := range []Version{Version1, Version2} {
		for _, uni := range []bool{false, true} {
			for _, loss := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/uni=%v/loss=%v", v, uni, loss), func(t *testing.T) {
					ct, st := dialTLS(t)
					l, err := ListenAddr("127.0.0.1:0", st, &Config{Versions: []Version{v}, EnableMultipath: true, EnableStreamResetPartialDelivery: true})
					if err != nil {
						t.Fatal(err)
					}
					defer l.Close()
					relay, drop, forwarded, dropped := relayPath(t, l.Addr().(*net.UDPAddr))
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					bind := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}
					client, err := DialAddr(ctx, l.Addr().String(), ct, &Config{Versions: []Version{v}, EnableMultipath: true, EnableStreamResetPartialDelivery: true, LocalAddr: bind})
					if err != nil {
						t.Fatal(err)
					}
					defer client.CloseWithError(0, "")
					server, err := l.Accept(ctx)
					if err != nil {
						t.Fatal(err)
					}
					for _, conn := range []*Conn{client, server} {
						state := conn.ConnectionState()
						if !state.SupportsMultipath || !state.SupportsStreamResetPartialDelivery.Local || !state.SupportsStreamResetPartialDelivery.Remote {
							t.Fatal("extensions not negotiated", state)
						}
					}
					path, err := client.OpenPath(ctx, 1, bind, relay)
					if err != nil {
						t.Fatal(err)
					}
					defer path.Close()
					if err := client.AbandonPath(ctx, 0); err != nil {
						t.Fatal(err)
					}

					before := forwarded.Load()
					drop.Store(loss)
					var tx *SendStream
					var bidi *Stream
					if uni {
						tx, err = client.OpenUniStreamSync(ctx)
					} else {
						bidi, err = client.OpenStreamSync(ctx)
						if err == nil {
							tx = bidi.SendStream
						}
					}
					if err != nil {
						t.Fatal(err)
					}
					deadline, _ := ctx.Deadline()
					tx.SetWriteDeadline(deadline)
					prefix := bytes.Repeat([]byte("reliable"), 4096)
					if n, err := tx.Write(prefix); err != nil || n != len(prefix) {
						t.Fatal("write", n, err)
					}
					tx.SetReliableBoundary()
					tx.CancelWrite(7)
					var rx *ReceiveStream
					var peerBidi *Stream
					if uni {
						rx, err = server.AcceptUniStream(ctx)
					} else {
						peerBidi, err = server.AcceptStream(ctx)
						if err == nil {
							rx = peerBidi.ReceiveStream
						}
					}
					if err != nil {
						t.Fatal(err)
					}
					rx.SetReadDeadline(deadline)
					got, err := io.ReadAll(rx)
					var reset *StreamError
					if !bytes.Equal(got, prefix) || !errors.As(err, &reset) || reset.ErrorCode != 7 || !reset.Remote {
						t.Fatal("reliable prefix", len(got), err)
					}
					if !uni {
						reply := []byte("reverse reliable prefix")
						peerBidi.SetWriteDeadline(deadline)
						if _, err := peerBidi.Write(reply); err != nil {
							t.Fatal(err)
						}
						peerBidi.SetReliableBoundary()
						peerBidi.CancelWrite(9)
						bidi.SetReadDeadline(deadline)
						got, err := io.ReadAll(bidi)
						if !bytes.Equal(got, reply) || !errors.As(err, &reset) || reset.ErrorCode != 9 || !reset.Remote {
							t.Fatal("reverse prefix", string(got), err)
						}
					}
					if err := tx.Release(ctx); err != nil {
						t.Fatal("sender release", err)
					}
					if err := rx.Release(ctx); err != nil {
						t.Fatal("receiver release", err)
					}
					if err := tx.WaitAcknowledged(ctx); !errors.As(err, &reset) || reset.ErrorCode != 7 || reset.Remote {
						t.Fatal("delivery error", err)
					}
					if forwarded.Load() <= before {
						t.Fatal("secondary path unused")
					}
					if loss && dropped.Load() == 0 {
						t.Fatal("loss fixture did not drop packets")
					}
				})
			}
		}
	}
}
