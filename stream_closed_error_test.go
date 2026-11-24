package quic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
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

// An empty Write sends nothing, but reports a send direction that has ended
// exactly as a Write with data would: after CancelWrite, after the peer's
// STOP_SENDING, and after the connection closes.
func TestEmptyWriteReportsEndedSend(t *testing.T) {
	for _, end := range []string{"CancelWrite", "STOP_SENDING", "connection"} {
		t.Run(end, func(t *testing.T) {
			clientTLS, serverTLS := dialTLS(t)
			l, err := ListenAddr("127.0.0.1:0", serverTLS, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := DialAddr(ctx, l.Addr().String(), clientTLS, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer client.server.Close()
			server, err := l.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			s, err := client.OpenStream()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Write([]byte("x")); err != nil {
				t.Fatal(err)
			}
			if n, err := s.Write(nil); n != 0 || err != nil {
				t.Fatal("empty write on an open stream:", n, err)
			}

			switch end {
			case "CancelWrite":
				s.CancelWrite(0x77)
			case "STOP_SENDING":
				peer, err := server.AcceptStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				peer.CancelRead(0x78)
			case "connection":
				server.CloseWithError(0x79, "gone")
			}
			select {
			case <-s.Context().Done():
			case <-ctx.Done():
				t.Fatal("send direction never ended")
			}
			_, want := s.Write([]byte("late"))
			if want == nil {
				t.Fatal("write with data succeeded after the send direction ended")
			}
			if n, err := s.Write(nil); n != 0 || err == nil || err.Error() != want.Error() {
				t.Fatalf("empty write: %d %v, want %v", n, err, want)
			}
			// Past the deadline too, an empty write reports what a write with
			// data does.
			s.SetWriteDeadline(time.Now().Add(-time.Second))
			_, wantLate := s.Write([]byte("late"))
			if n, err := s.Write(nil); n != 0 || err == nil || wantLate == nil || err.Error() != wantLate.Error() {
				t.Fatalf("empty write past the deadline: %d %v, want %v", n, err, wantLate)
			}
			s.SetWriteDeadline(time.Time{})
			wantTry := s.TryWriteAll([]byte("late"))
			if wantTry == nil {
				t.Fatal("TryWriteAll with data succeeded after the send direction ended")
			}
			if err := s.TryWriteAll(nil); err == nil || err.Error() != wantTry.Error() {
				t.Fatalf("empty TryWriteAll: %v, want %v", err, wantTry)
			}
		})
	}
}

// Once sending has ended, every send call reports why, even with its write
// deadline expired: a reset or a closed connection is not a timeout, and a
// caller must not be told it may retry. The answer must not depend on which
// of two ready events a select happens to pick, so each call is repeated.
// On a stream still open, an expired deadline is reported as before.
func TestEndedSendBeatsExpiredDeadline(t *testing.T) {
	for _, end := range []string{"open", "CancelWrite", "STOP_SENDING", "connection", "endpoint closed"} {
		t.Run(end, func(t *testing.T) {
			clientTLS, serverTLS := dialTLS(t)
			l, err := ListenAddr("127.0.0.1:0", serverTLS, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := DialAddr(ctx, l.Addr().String(), clientTLS, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer client.server.Close()
			server, err := l.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			s, err := client.OpenStream()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Write([]byte("x")); err != nil {
				t.Fatal(err)
			}
			var want func(error) bool
			switch end {
			case "open":
				want = func(err error) bool { return errors.Is(err, os.ErrDeadlineExceeded) }
			case "CancelWrite":
				s.CancelWrite(0x77)
				want = func(err error) bool { var reset *StreamError; return errors.As(err, &reset) && reset.ErrorCode == 0x77 }
			case "STOP_SENDING":
				peer, err := server.AcceptStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				peer.CancelRead(0x78)
				want = func(err error) bool { var reset *StreamError; return errors.As(err, &reset) && reset.ErrorCode == 0x78 }
			case "connection", "endpoint closed":
				server.CloseWithError(0x79, "gone")
				want = func(err error) bool {
					var closed *ApplicationError
					return errors.As(err, &closed) && closed.ErrorCode == 0x79
				}
			}
			if end != "open" {
				select {
				case <-s.Context().Done():
				case <-ctx.Done():
					t.Fatal("send direction never ended")
				}
			}
			if end == "endpoint closed" {
				select {
				case <-client.server.Done():
				case <-ctx.Done():
					t.Fatal("client endpoint never closed")
				}
			}
			s.SetWriteDeadline(time.Now().Add(-time.Second))
			for i := 0; i < 50; i++ {
				for name, call := range map[string]func() error{
					"Write":       func() error { _, err := s.Write([]byte("late")); return err },
					"empty Write": func() error { _, err := s.Write(nil); return err },
					"Close":       s.Close,
				} {
					if end == "open" && name == "Close" {
						continue // a Close past the deadline is covered below
					}
					err := call()
					// An empty write waits for nothing, so on an open stream
					// the deadline does not apply to it.
					if end == "open" && name == "empty Write" {
						if err != nil {
							t.Fatal("empty Write on an open stream past the deadline:", err)
						}
						continue
					}
					if !want(err) {
						t.Fatalf("%s past the deadline: %v", name, err)
					}
				}
			}
			if end == "open" {
				if err := s.Close(); !want(err) {
					t.Fatal("Close past the deadline:", err)
				}
			}
		})
	}
}

// A repeat Close past its deadline, waiting behind another call, still
// reports success: sending ended with this stream's own FIN.
func TestRepeatCloseBehindBusyGateSucceeds(t *testing.T) {
	clientTLS, serverTLS := dialTLS(t)
	l, err := ListenAddr("127.0.0.1:0", serverTLS, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := DialAddr(ctx, l.Addr().String(), clientTLS, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.server.Close()
	if _, err := l.Accept(ctx); err != nil {
		t.Fatal(err)
	}
	s, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.Context().Done():
	case <-ctx.Done():
		t.Fatal("send context never ended after Close")
	}
	s.gate <- struct{}{} // another call holds the gate
	defer s.gate.release()
	s.SetWriteDeadline(time.Now().Add(-time.Second))
	if err := s.Close(); err != nil {
		t.Fatal("repeat Close behind a busy gate:", err)
	}
}
