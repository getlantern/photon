package quic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"
)

func TestPublicPeekPreservesCredit(t *testing.T) {
	for _, v := range []Version{Version1, Version2} {
		for _, serverSends := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/server=%t", v, serverSends), func(t *testing.T) {
				ct, st := dialTLS(t)
				cfg := &Config{Versions: []Version{v}, InitialStreamReceiveWindow: 4, MaxStreamReceiveWindow: 4, InitialConnectionReceiveWindow: 4, MaxConnectionReceiveWindow: 4}
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
				tx, err := sender.OpenUniStream()
				if err != nil {
					t.Fatal(err)
				}
				if n, err := tx.Write([]byte("ab")); n != 2 || err != nil {
					t.Fatal(n, err)
				}
				rx, err := receiver.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				rx.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
				b := []byte("xxxxx")
				if n, err := rx.Peek(b[:3]); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) || string(b) != "xxxxx" {
					t.Fatal("incomplete peek", n, err, string(b))
				}
				rx.SetReadDeadline(time.Now().Add(time.Second))
				if n, err := tx.Write([]byte("cd")); n != 2 || err != nil {
					t.Fatal(n, err)
				}
				for i := 0; i < 2; i++ {
					if n, err := rx.Peek(b[:3]); n != 3 || err != nil || string(b[:3]) != "abc" {
						t.Fatal(n, err, string(b))
					}
				}
				tx.SetWriteDeadline(time.Now().Add(30 * time.Millisecond))
				if n, err := tx.Write([]byte("e")); n != 0 || !errors.Is(err, os.ErrDeadlineExceeded) {
					t.Fatal("peek granted credit", n, err)
				}
				tx.SetWriteDeadline(time.Now().Add(time.Second))
				if err := tx.Close(); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 2; i++ {
					if n, err := rx.Peek(b); n != 4 || !errors.Is(err, io.EOF) || string(b[:4]) != "abcd" {
						t.Fatal("terminal peek", n, err, string(b))
					}
				}
				got, err := io.ReadAll(rx)
				if err != nil || string(got) != "abcd" {
					t.Fatal("read after peek", string(got), err)
				}
			})
		}
	}
}
