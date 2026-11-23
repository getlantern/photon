package interop

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

func TestWriteDeadlineChangeAfterPartialCommit(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, expire := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/expire=%v", uint32(v), expire), func(t *testing.T) {
				clientTLS, serverTLS := tlsConfigs(t)
				listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				peer, err := dialUpstreamAddr(t, ctx, listener.Addr().String(), clientTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}, InitialStreamReceiveWindow: 16384, MaxStreamReceiveWindow: 16384})
				if err != nil {
					t.Fatal(err)
				}
				defer peer.CloseWithError(0, "")
				conn, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				tx, err := conn.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				payload := make([]byte, 48*1024)
				for i := range payload {
					payload[i] = byte(i*31 + i/257)
				}
				type result struct {
					n   int
					err error
				}
				done := make(chan result, 1)
				tx.SetWriteDeadline(time.Now().Add(4 * time.Second))
				go func() { n, err := tx.Write(payload); done <- result{n, err} }()
				rx, err := peer.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				rx.SetReadDeadline(time.Now().Add(4 * time.Second))
				first := make([]byte, 1)
				if _, err := io.ReadFull(rx, first); err != nil {
					t.Fatal(err)
				}
				// Receipt proves partial progress. The fixed window cannot admit the full
				// payload until the peer reads more, so deadline changes affect this Write.
				select {
				case got := <-done:
					t.Fatalf("write completed before window opened: %+v", got)
				default:
				}
				tx.SetWriteDeadline(time.Now().Add(3 * time.Second))
				committed := 0
				if expire {
					tx.SetWriteDeadline(time.Now().Add(-time.Second))
					select {
					case got := <-done:
						if !errors.Is(got.err, os.ErrDeadlineExceeded) || got.n <= 0 || got.n >= len(payload) {
							t.Fatalf("partial result: %+v", got)
						}
						committed = got.n
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				tx.SetWriteDeadline(time.Time{})
				if expire {
					go func() { n, err := tx.Write(payload[committed:]); done <- result{n, err} }()
				}
				// Read exactly the payload before waiting for FIN, allowing flow control
				// to advance while the original (or resumed) write is still executing.
				got := make([]byte, len(payload))
				got[0] = first[0]
				if _, err := io.ReadFull(rx, got[1:]); err != nil {
					t.Fatal(err)
				}
				select {
				case r := <-done:
					if r.err != nil || r.n != len(payload)-committed {
						t.Fatalf("write result: %+v", r)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if err := tx.Close(); err != nil {
					t.Fatal(err)
				}
				tail, err := io.ReadAll(rx)
				if err != nil || len(tail) != 0 || !bytes.Equal(got, payload) {
					t.Fatalf("corrupted or duplicated write: tail=%d err=%v", len(tail), err)
				}
			})
		}
	}
}
