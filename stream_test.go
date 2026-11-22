package quic

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func TestStreamDeadlineChanges(t *testing.T) {
	var d streamDeadline
	entered := make(chan context.Context, 2)
	result := make(chan error, 1)
	go func() {
		result <- d.run(func(ctx context.Context) error { entered <- ctx; <-ctx.Done(); return ctx.Err() })
	}()
	first := <-entered
	d.set(time.Now().Add(time.Hour))
	select {
	case <-first.Done():
	case <-time.After(time.Second):
		t.Fatal("old waiter not interrupted")
	}
	second := <-entered
	d.set(time.Now().Add(-time.Second))
	select {
	case <-second.Done():
	case <-time.After(time.Second):
		t.Fatal("replacement waiter not interrupted")
	}
	select {
	case err := <-result:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("deadline did not expire")
	}
	d.set(time.Time{})
	if err := d.run(func(ctx context.Context) error {
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestStreamDeadlineCommittedResult(t *testing.T) {
	var d streamDeadline
	for _, outcome := range []error{nil, errors.New("committed error")} {
		calls := 0
		err := d.run(func(context.Context) error { calls++; d.set(time.Now()); return outcome })
		if err != outcome || calls != 1 {
			t.Fatal("retried committed operation", err, calls)
		}
	}
}
