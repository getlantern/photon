package quic

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"
)

func TestPublicReceiveFinalSizeCallback(t *testing.T) {
	for _, v := range []Version{Version1, Version2} {
		for _, serverSends := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/server=%t", v, serverSends), func(t *testing.T) {
				ct, st := dialTLS(t)
				l, err := ListenAddr("127.0.0.1:0", st, &Config{Versions: []Version{v}})
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
				s, err := l.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				sender, receiver := c, s
				if serverSends {
					sender, receiver = s, c
				}
				var completed *ReceiveStream
				for _, mode := range []string{"replace", "remove", "unknown-close"} {
					tx, err := sender.OpenUniStream()
					if err != nil {
						t.Fatal(err)
					}
					if _, err := tx.Write([]byte("prefix")); err != nil {
						t.Fatal(err)
					}
					rx, err := receiver.AcceptUniStream(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if mode == "replace" {
						completed = rx
					}
					events := make(chan int64, 4)
					rx.SetReceiveFinalSizeCallback(func(size int64) { events <- -size })
					if mode == "replace" {
						rx.SetReceiveFinalSizeCallback(func(size int64) { events <- size })
					}
					if mode == "remove" {
						rx.SetReceiveFinalSizeCallback(nil)
					}
					if mode == "unknown-close" {
						if err := sender.CloseWithError(0, "done"); err != nil {
							t.Fatal(err)
						}
						select {
						case <-receiver.Context().Done():
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
						receiver.server.Close()
						late := int64(-1)
						completed.SetReceiveFinalSizeCallback(func(size int64) { late = size })
						if late != 6 {
							t.Fatal("lost known final size after runtime shutdown", late)
						}
						rx.SetReceiveFinalSizeCallback(func(size int64) { events <- size })
						select {
						case n := <-events:
							t.Fatal("unknown size notified", n)
						default:
						}
						continue
					}
					if err := tx.Close(); err != nil {
						t.Fatal(err)
					}
					rx.SetReadDeadline(time.Now().Add(time.Second))
					if b, err := io.ReadAll(rx); err != nil || string(b) != "prefix" {
						t.Fatal(string(b), err)
					}
					if mode == "replace" {
						select {
						case n := <-events:
							if n != 6 {
								t.Fatal(n)
							}
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
					}
					select {
					case n := <-events:
						t.Fatal("removed/duplicate callback", n)
					default:
					}
					immediate := false
					rx.SetReceiveFinalSizeCallback(func(n int64) {
						immediate = true
						if n != 6 {
							t.Fatal(n)
						}
						// A synchronous known-size callback can call QUIC methods without
						// blocking the connection owner on its own command queue.
						rx.SetReceiveFinalSizeCallback(nil)
					})
					if !immediate {
						t.Fatal("late registration was not synchronous")
					}
				}
			})
		}
	}
}
