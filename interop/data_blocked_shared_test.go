package interop

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlogwriter"
)

func TestDataBlockedSharedRecoveryUpstream(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, serverRole := range []bool{false, true} {
			for _, uni := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/server=%t/uni=%t", version, serverRole, uni), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					ct, st := tlsConfigs(t)
					trace := &connectionDataBlockedTrace{frames: make(chan connectionBlockedObservation, 8)}
					var own *quic.Conn
					var peer *upstream.Conn
					peerConfig := &upstream.Config{InitialConnectionReceiveWindow: 2, MaxConnectionReceiveWindow: 2, InitialStreamReceiveWindow: 16, MaxStreamReceiveWindow: 16, Versions: []upstream.Version{upstream.Version(version)}, Tracer: func(context.Context, bool, upstream.ConnectionID) qlogwriter.Trace { return trace }}
					ownConfig := &quic.Config{Versions: []quic.Version{version}}
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
						SetWriteDeadline(time.Time) error
						StreamID() quic.StreamID
						Close() error
					}
					type receiver interface {
						Read([]byte) (int, error)
						SetReadDeadline(time.Time) error
						StreamID() upstream.StreamID
					}
					var tx [2]sender
					var rx [2]receiver
					for i := range tx {
						var err error
						if uni {
							tx[i], err = own.OpenUniStream()
						} else {
							tx[i], err = own.OpenStream()
						}
						if err != nil {
							t.Fatal(err)
						}
						if err := tx[i].SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
							t.Fatal(err)
						}
						if n, err := tx[i].Write([]byte{byte(i + 1)}); n != 1 || err != nil {
							t.Fatal("initial write", n, err)
						}
						if uni {
							rx[i], err = peer.AcceptUniStream(ctx)
						} else {
							rx[i], err = peer.AcceptStream(ctx)
						}
						if err != nil {
							t.Fatal(err)
						}
						if uint64(rx[i].StreamID()) != uint64(tx[i].StreamID()) {
							t.Fatal("stream ID mismatch")
						}
						if err := rx[i].SetReadDeadline(time.Now().Add(4 * time.Second)); err != nil {
							t.Fatal(err)
						}
					}
					blockBoth := func() {
						t.Helper()
						for _, s := range tx {
							if err := s.SetWriteDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
								t.Fatal(err)
							}
							if n, err := s.Write([]byte{9}); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
								t.Fatal("shared blocked write", n, err)
							}
						}
					}
					observe := func(want uint64) {
						t.Helper()
						for {
							select {
							case f := <-trace.frames:
								if f.limit == want {
									return
								}
								if f.limit > want {
									t.Fatalf("unexpected blocked limit %d, want %d", f.limit, want)
								}
							case <-ctx.Done():
								t.Fatalf("missing DATA_BLOCKED at %d: %v", want, ctx.Err())
							}
						}
					}
					blockBoth()
					observe(2)
					// Only application consumption grants the next two bytes. Each stream
					// retains ample stream credit, isolating connection-wide wakeups.
					for i, s := range rx {
						var b [1]byte
						if _, err := io.ReadFull(s, b[:]); err != nil || b[0] != byte(i+1) {
							t.Fatal("initial data", b, err)
						}
					}
					for i, s := range tx {
						if err := s.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
							t.Fatal(err)
						}
						if n, err := s.Write([]byte{byte(i + 3)}); n != 1 || err != nil {
							t.Fatal("write after shared grant", n, err)
						}
					}
					blockBoth()
					observe(4)
					// FIN does not need additional connection credit. Check exact delivery and
					// EOF after closing both senders while connection credit is exhausted.
					for _, s := range tx {
						if err := s.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
							t.Fatal(err)
						}
						if err := s.Close(); err != nil {
							t.Fatal(err)
						}
					}
					for i, s := range rx {
						b, err := io.ReadAll(s)
						if err != nil || len(b) != 1 || b[0] != byte(i+3) {
							t.Fatal("final data", b, err)
						}
					}
				})
			}
		}
	}
}
