package quic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	engineStream "github.com/getlantern/photon/internal/stream"
)

func TestWriteAfterCloseReturnsErrStreamClosed(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		for _, uni := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/uni=%t", version, uni), func(t *testing.T) {
				clientTLS, serverTLS := dialTLS(t)
				l, err := ListenAddr("127.0.0.1:0", serverTLS, &Config{Versions: []Version{version}})
				if err != nil {
					t.Fatal(err)
				}
				defer l.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				client, err := DialAddr(ctx, l.Addr().String(), clientTLS, &Config{Versions: []Version{version}})
				if err != nil {
					t.Fatal(err)
				}
				defer client.server.Close()
				server, err := l.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				var reply *ReceiveStream
				open := func() *SendStream {
					t.Helper()
					if uni {
						s, err := client.OpenUniStream()
						if err != nil {
							t.Fatal(err)
						}
						return s
					}
					s, err := client.OpenStream()
					if err != nil {
						t.Fatal(err)
					}
					reply = s.ReceiveStream
					return s.SendStream
				}
				assertClosed := func(s *SendStream, when string) {
					t.Helper()
					for name, write := range map[string]func() error{
						"Write":          func() error { _, err := s.Write([]byte("late")); return err },
						"empty Write":    func() error { _, err := s.Write(nil); return err },
						"WriteWithLimit": func() error { _, err := s.WriteWithLimit([]byte("late"), func(n int) int { return n }); return err },
						"TryWriteAll":    func() error { return s.TryWriteAll([]byte("late")) },
					} {
						if err := write(); err != ErrStreamClosed || !errors.Is(err, engineStream.ErrSendClosed) {
							t.Fatalf("%s %s: %v", name, when, err)
						}
					}
				}
				s := open()
				if _, err := s.Write([]byte("data")); err != nil {
					t.Fatal(err)
				}
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
				assertClosed(s, "after Close")
				if err := s.Close(); err != nil {
					t.Fatal("repeated Close", err)
				}
				if cause := context.Cause(s.Context()); cause != ErrStreamClosed {
					t.Fatal("close cause", cause)
				}
				// The error survives delivery and reclamation of the stream.
				var received *ReceiveStream
				if uni {
					received, err = server.AcceptUniStream(ctx)
				} else {
					var accepted *Stream
					if accepted, err = server.AcceptStream(ctx); err == nil {
						received = accepted.ReceiveStream
						err = accepted.Close()
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				received.SetReadDeadline(time.Now().Add(4 * time.Second))
				if b, err := io.ReadAll(received); err != nil || string(b) != "data" {
					t.Fatalf("delivered %q: %v", b, err)
				}
				if err := s.WaitAcknowledged(ctx); err != nil {
					t.Fatal(err)
				}
				assertClosed(s, "after delivery")
				if reply != nil {
					reply.SetReadDeadline(time.Now().Add(4 * time.Second))
					if b, err := io.ReadAll(reply); err != nil || len(b) != 0 {
						t.Fatalf("reply %q: %v", b, err)
					}
				}
				if err := s.Release(ctx); err != nil {
					t.Fatal(err)
				}
				assertClosed(s, "after release")

				// Cancellation is not a close: it keeps reporting the stream error.
				canceled := open()
				canceled.CancelWrite(7)
				var streamErr *StreamError
				if _, err := canceled.Write([]byte("late")); !errors.As(err, &streamErr) || streamErr.ErrorCode != 7 || errors.Is(err, ErrStreamClosed) {
					t.Fatal("write after CancelWrite", err)
				}
			})
		}
	}
}
