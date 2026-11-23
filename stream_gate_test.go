package quic

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/transport"
)

func gatePair(t *testing.T, ctx context.Context) (*Conn, *Conn) {
	ct, st := dialTLS(t)
	cfg := &Config{InitialStreamReceiveWindow: 3, MaxStreamReceiveWindow: 3, InitialConnectionReceiveWindow: 3, MaxConnectionReceiveWindow: 3}
	l, err := ListenAddr("127.0.0.1:0", st, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	c, err := DialAddr(ctx, l.Addr().String(), ct, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.CloseWithError(0, "") })
	s, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return c, s
}

func TestWaitAcknowledgedCanceledBehindBlockedWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _ := gatePair(t, ctx)
	tx, err := c.OpenUniStream()
	if err != nil {
		t.Fatal(err)
	}
	admitted := make(chan struct{}, 1)
	written := make(chan error, 1)
	go func() {
		// The peer never reads, so the suffix waits for credit holding the gate.
		_, err := tx.WriteWithLimit([]byte("abcdef"), func(maximum int) int {
			select {
			case admitted <- struct{}{}:
			default:
			}
			return maximum
		})
		written <- err
	}()
	select {
	case <-admitted:
	case <-ctx.Done():
		t.Fatal("no admission", ctx.Err())
	}
	for _, expired := range []bool{false, true} {
		waitCtx, stop := context.WithCancel(context.Background())
		if expired {
			stop()
		} else {
			time.AfterFunc(50*time.Millisecond, stop)
		}
		acknowledged := make(chan error, 1)
		go func() { acknowledged <- tx.WaitAcknowledged(waitCtx) }()
		select {
		case err := <-acknowledged:
			if !errors.Is(err, context.Canceled) {
				t.Fatal("canceled wait", expired, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("canceled wait stuck behind blocked write", expired)
		}
		stop()
	}
	select {
	case err := <-written:
		t.Fatal("write was not blocked", err)
	default:
	}
	tx.CancelWrite(1)
	select {
	case <-written:
	case <-ctx.Done():
		t.Fatal("blocked write did not finish", ctx.Err())
	}
}

func TestWriteDeadlineBehindAcknowledgmentWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, s := gatePair(t, ctx)
	tx, err := c.OpenUniStream()
	if err != nil {
		t.Fatal(err)
	}
	// Park the peer's owner so nothing is acknowledged while the test runs.
	parked, resume := make(chan struct{}), make(chan struct{})
	go s.server.Do(context.Background(), func(*transport.ServerEndpoint) error {
		close(parked)
		<-resume
		return nil
	})
	defer close(resume)
	select {
	case <-parked:
	case <-ctx.Done():
		t.Fatal("peer owner not parked", ctx.Err())
	}
	if err := tx.TryWriteAll([]byte("a")); err != nil {
		t.Fatal(err)
	}
	waitCtx, stop := context.WithCancel(context.Background())
	defer stop()
	acknowledged := make(chan error, 1)
	go func() { acknowledged <- tx.WaitAcknowledged(waitCtx) }()
	for len(tx.gate) == 0 {
		if ctx.Err() != nil {
			t.Fatal("acknowledgment wait never took the gate")
		}
		time.Sleep(time.Millisecond)
	}
	for _, operation := range []struct {
		name string
		run  func() error
	}{
		{"write", func() error { _, err := tx.Write([]byte("b")); return err }},
		{"close", tx.Close},
	} {
		for _, expired := range []bool{false, true} {
			if expired {
				tx.SetWriteDeadline(time.Now().Add(-time.Second))
			} else {
				tx.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
			}
			done := make(chan error, 1)
			go func() { done <- operation.run() }()
			select {
			case err := <-done:
				if !errors.Is(err, os.ErrDeadlineExceeded) {
					t.Fatal(operation.name, expired, err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal(operation.name, "deadline ignored behind acknowledgment wait", expired)
			}
		}
	}
	// A deadline set while queued for the gate must also reach the waiter.
	tx.SetWriteDeadline(time.Time{})
	done := make(chan error, 1)
	go func() { _, err := tx.Write([]byte("b")); done <- err }()
	select {
	case err := <-done:
		t.Fatal("write passed a held gate", err)
	case <-time.After(20 * time.Millisecond):
	}
	tx.SetWriteDeadline(time.Now().Add(-time.Second))
	select {
	case err := <-done:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatal("deadline change", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("deadline change ignored behind acknowledgment wait")
	}
	select {
	case err := <-acknowledged:
		t.Fatal("acknowledgment wait was not stalled", err)
	default:
	}
	stop()
	select {
	case err := <-acknowledged:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("acknowledgment wait did not finish", ctx.Err())
	}
	// Nothing queued behind the gate may have been committed.
	tx.SetWriteDeadline(time.Time{})
	if err := tx.TryWriteAll([]byte("cd")); err != nil {
		t.Fatal("refused writes consumed credit", err)
	}
}

func TestStreamDeadlineReachesEveryWaiter(t *testing.T) {
	var d streamDeadline
	const waiters = 3
	entered := make(chan struct{}, waiters)
	results := make(chan error, waiters)
	for range waiters {
		go func() {
			results <- d.run(func(ctx context.Context) error {
				entered <- struct{}{}
				<-ctx.Done()
				return ctx.Err()
			})
		}()
	}
	for range waiters {
		<-entered
	}
	d.set(time.Now().Add(-time.Second))
	for range waiters {
		select {
		case err := <-results:
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("waiter missed deadline change")
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.cancels) != 0 {
		t.Fatal("leaked cancellations", len(d.cancels))
	}
}
