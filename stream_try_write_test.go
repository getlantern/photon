package quic

import (
	"context"
	"errors"
	"fmt"
	"io"

	"testing"
	"time"
)

func TestPublicTryWriteAllAdmission(t *testing.T) {
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
				tx.SetWriteDeadline(time.Now().Add(-time.Second))
				if err := tx.TryWriteAll([]byte("reject")); !errors.Is(err, ErrWouldBlock) {
					t.Fatal("partial flow admission", err)
				}
				if err := tx.TryWriteAll([]byte("abcd")); err != nil {
					t.Fatal("atomic admission ignored deadline", err)
				}
				if err := tx.TryWriteAll([]byte("e")); !errors.Is(err, ErrWouldBlock) {
					t.Fatal("exhausted credit", err)
				}
				if err := tx.TryWriteAll(nil); err != nil {
					t.Fatal("empty try", err)
				}
				tx.SetWriteDeadline(time.Now().Add(time.Second))
				if err := tx.Close(); err != nil {
					t.Fatal(err)
				}
				rx, err := receiver.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				rx.SetReadDeadline(time.Now().Add(time.Second))
				got, err := io.ReadAll(rx)
				if err != nil || string(got) != "abcd" {
					t.Fatal("rejected prefix leaked", string(got), err)
				}
				if err := tx.TryWriteAll(nil); err == nil || errors.Is(err, ErrWouldBlock) {
					t.Fatal("closed try", err)
				}
			})
		}
	}
}

func TestTryWriteAllBusyWriter(t *testing.T) {
	s := newSendStream(nil)
	if !s.gate.tryAcquire() {
		t.Fatal("idle gate")
	}
	defer s.gate.release()
	if err := s.TryWriteAll([]byte("busy")); !errors.Is(err, ErrWouldBlock) {
		t.Fatal(err)
	}
}
