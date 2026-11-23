package quic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"
)

func TestCompletedReadsAfterRuntimeStops(t *testing.T) {
	for _, v := range []Version{Version1, Version2} {
		t.Run(fmt.Sprintf("%x", v), func(t *testing.T) {
			ct, st := dialTLS(t)
			l, err := ListenAddr("127.0.0.1:0", st, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			c, err := DialAddr(ctx, l.Addr().String(), ct, &Config{Versions: []Version{v}})
			if err != nil {
				t.Fatal(err)
			}
			defer c.CloseWithError(0, "")
			peer, err := l.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var reads []*ReceiveStream
			for i := 0; i < 4; i++ {
				tx, err := peer.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Write([]byte("retained payload")); err != nil {
					t.Fatal(err)
				}
				if err := tx.Close(); err != nil {
					t.Fatal(err)
				}
				rx, err := c.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				reads = append(reads, rx)
				if err := tx.WaitAcknowledged(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if err := peer.CloseWithError(0x100, "finished"); err != nil {
				t.Fatal(err)
			}
			select {
			case <-c.Context().Done():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			// Observe actual owner termination, not a delay that assumes it happened.
			select {
			case <-c.server.Done():
			case <-ctx.Done():
				t.Fatal("runtime did not stop", ctx.Err())
			}
			var wg sync.WaitGroup
			for index, rx := range reads {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if index == 0 {
						rx.CancelRead(17)
						_, err := rx.Read(make([]byte, 1))
						var reset *StreamError
						if !errors.As(err, &reset) || reset.ErrorCode != 17 {
							t.Errorf("late cancellation: %v", err)
						}
						if err := rx.Release(ctx); err != nil {
							t.Error("canceled release", err)
						}
						return
					}
					if index == 1 {
						rx.SetReadDeadline(time.Now().Add(-time.Second))
						if _, err := rx.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
							t.Errorf("late read deadline: %v", err)
						}
						rx.SetReadDeadline(time.Time{})
					}
					peeked := make([]byte, len("retained payload")+1)
					if n, err := rx.Peek(peeked); n != len("retained payload") || !errors.Is(err, io.EOF) || string(peeked[:n]) != "retained payload" {
						t.Errorf("late peek %q: %v", peeked, err)
						return
					}
					b, err := io.ReadAll(rx)
					if err != nil || string(b) != "retained payload" {
						t.Errorf("late read %q: %v", b, err)
						return
					}
					if err := rx.Release(ctx); err != nil {
						t.Error("release", err)
					}
				}()
			}
			wg.Wait()
		})
	}
}
