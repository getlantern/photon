package quic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

func TestWriteLimiterBlockedLifecycle(t *testing.T) {
	for _, v := range []Version{Version1, Version2} {
		for _, serverSends := range []bool{false, true} {
			for _, uni := range []bool{false, true} {
				for _, mode := range []string{"deadline", "deadline-change", "cancel"} {
					t.Run(fmt.Sprintf("%x/server=%t/uni=%t/%s", v, serverSends, uni, mode), func(t *testing.T) {
						ct, st := dialTLS(t)
						cfg := &Config{Versions: []Version{v}, InitialStreamReceiveWindow: 3, MaxStreamReceiveWindow: 3, InitialConnectionReceiveWindow: 3, MaxConnectionReceiveWindow: 3}
						l, err := ListenAddr("127.0.0.1:0", st, cfg)
						if err != nil {
							t.Fatal(err)
						}
						defer l.Close()
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						c, err := DialAddr(ctx, l.Addr().String(), ct, cfg)
						if err != nil {
							t.Fatal(err)
						}
						defer c.CloseWithError(0, "")
						s, err := l.Accept(ctx)
						if err != nil {
							t.Fatal(err)
						}
						sender, receiver := c, s
						if serverSends {
							sender, receiver = s, c
						}
						var tx interface {
							WriteWithLimit([]byte, func(int) int) (int, error)
							SetWriteDeadline(time.Time) error
							CancelWrite(StreamErrorCode)
							Close() error
						}
						if uni {
							tx, err = sender.OpenUniStream()
						} else {
							tx, err = sender.OpenStream()
						}
						if err != nil {
							t.Fatal(err)
						}
						deadline := time.Now().Add(time.Second)
						if mode == "deadline" {
							deadline = time.Now().Add(100 * time.Millisecond)
						}
						tx.SetWriteDeadline(deadline)
						var calls, charged atomic.Int64
						admitted := make(chan struct{}, 1)
						type result struct {
							n   int
							err error
						}
						done := make(chan result, 1)
						go func() {
							n, err := tx.WriteWithLimit([]byte("abcdef"), func(maximum int) int {
								calls.Add(1)
								charged.Add(int64(maximum))
								select {
								case admitted <- struct{}{}:
								default:
								}
								return maximum
							})
							done <- result{n, err}
						}()
						select {
						case <-admitted:
						case <-ctx.Done():
							t.Fatal("no admission", ctx.Err())
						}
						if mode == "cancel" {
							tx.CancelWrite(77)
						} else if mode == "deadline-change" {
							tx.SetWriteDeadline(time.Now().Add(-time.Second))
						}
						var got result
						select {
						case got = <-done:
						case <-ctx.Done():
							t.Fatal("blocked write did not finish", ctx.Err())
						}
						if got.n != 3 || calls.Load() != 1 || charged.Load() != 3 {
							t.Fatal("prefix accounting", got, calls.Load(), charged.Load())
						}
						if mode == "cancel" {
							var canceled *StreamError
							if !errors.As(got.err, &canceled) || canceled.ErrorCode != 77 || canceled.Remote {
								t.Fatal("cancellation cause", got.err)
							}
							tx.SetWriteDeadline(time.Now().Add(time.Second))
							invoked := false
							n, err := tx.WriteWithLimit([]byte("def"), func(maximum int) int { invoked = true; return maximum })
							if n != 0 || !errors.As(err, &canceled) || canceled.ErrorCode != 77 || invoked {
								t.Fatal("canceled retry", n, err, invoked)
							}
							return
						}
						if !errors.Is(got.err, os.ErrDeadlineExceeded) {
							t.Fatal("deadline cause", got.err)
						}
						var rx interface {
							Read([]byte) (int, error)
							SetReadDeadline(time.Time) error
						}
						if uni {
							rx, err = receiver.AcceptUniStream(ctx)
						} else {
							rx, err = receiver.AcceptStream(ctx)
						}
						if err != nil {
							t.Fatal(err)
						}
						rx.SetReadDeadline(time.Now().Add(time.Second))
						prefix := make([]byte, 3)
						if _, err := io.ReadFull(rx, prefix); err != nil || string(prefix) != "abc" {
							t.Fatal("committed prefix", string(prefix), err)
						}
						tx.SetWriteDeadline(time.Now().Add(time.Second))
						offered := 0
						n, err := tx.WriteWithLimit([]byte("def"), func(maximum int) int { offered = maximum; return 2 })
						if n != 2 || !errors.Is(err, ErrWriteLimitReached) || offered != 3 {
							t.Fatal("external limit after new grant", n, err, offered)
						}
						if n, err := tx.WriteWithLimit([]byte("f"), nil); n != 1 || err != nil {
							t.Fatal("nil limiter suffix", n, err)
						}
						if err := tx.Close(); err != nil {
							t.Fatal(err)
						}
						tail, err := io.ReadAll(rx)
						if err != nil || string(tail) != "def" {
							t.Fatal("exact resumed data", string(tail), err)
						}
					})
				}
			}
		}
	}
}
