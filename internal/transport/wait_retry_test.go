package transport

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestWaitRepeatedWakeAndCommittedCancellation(t *testing.T) {
	s := testUDPServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	operation, stop := context.WithCancel(ctx)
	defer stop()
	calls := make(chan int, 1)
	done := make(chan error, 1)
	const retries = 32
	go func() {
		attempt := 0
		done <- s.Wait(operation, func(*ServerEndpoint) (bool, error) {
			attempt++
			calls <- attempt
			if attempt == retries+1 {
				// Cancellation after commitment must not discard success or retry.
				stop()
				return true, nil
			}
			return false, nil
		})
	}()
	for want := 1; want <= retries+1; want++ {
		select {
		case got := <-calls:
			if got != want {
				t.Fatalf("attempt %d, want %d", got, want)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if want <= retries {
			if err := s.Do(ctx, func(*ServerEndpoint) error { return nil }); err != nil {
				t.Fatal(err)
			}
		}
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	called := false
	if err := s.Wait(ctx, func(*ServerEndpoint) (bool, error) { called = true; return true, nil }); err != nil || !called {
		t.Fatalf("subsequent operation: called=%v err=%v", called, err)
	}
}

// Isolate command submission/retry overhead from socket I/O and packet parsing.
// The owner returns an already signaled notification to force readiness retries.
func BenchmarkWaitRetries(b *testing.B) {
	for _, attempts := range []int{1, 8, 32} {
		b.Run(fmt.Sprint(attempts), func(b *testing.B) {
			s := &UDPServer{commands: make(chan serverCommand), done: make(chan struct{})}
			changed := make(chan struct{})
			close(changed)
			joined := make(chan struct{})
			go func() {
				defer close(joined)
				for cmd := range s.commands {
					err := cmd.run(nil)
					cmd.reply <- serverResult{err: err, changed: changed}
				}
			}()
			defer func() { close(s.commands); <-joined }()
			remaining := 0
			run := func(*ServerEndpoint) (bool, error) { remaining--; return remaining == 0, nil }
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				remaining = attempts
				if err := s.Wait(context.Background(), run); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}
