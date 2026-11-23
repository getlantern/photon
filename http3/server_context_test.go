package http3

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestServerContextLifetimeBinding(t *testing.T) {
	for _, which := range []string{"owner", "callback", "already-owner"} {
		t.Run(which, func(t *testing.T) {
			type key struct{}
			base, stopBase := context.WithCancelCause(context.Background())
			defer stopBase(context.Canceled)
			selected, stopSelected := context.WithCancelCause(context.WithValue(context.Background(), key{}, "custom"))
			defer stopSelected(context.Canceled)
			cause := errors.New("selected cause")
			if which == "already-owner" {
				stopBase(cause)
			}
			ctx, cleanup := bindServerContext(base, selected)
			defer cleanup()
			if ctx.Value(key{}) != "custom" {
				t.Fatal("values lost")
			}
			if which == "owner" {
				stopBase(cause)
			} else if which == "callback" {
				stopSelected(cause)
			}
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
				t.Fatal("cancellation detached")
			}
			if !errors.Is(context.Cause(ctx), cause) {
				t.Fatal(context.Cause(ctx))
			}
		})
	}
}

func TestServerContextSelectedDeadline(t *testing.T) {
	cause := errors.New("callback deadline")
	deadline := time.Now().Add(20 * time.Millisecond)
	selected, stop := context.WithDeadlineCause(context.Background(), deadline, cause)
	defer stop()
	ctx, cleanup := bindServerContext(context.Background(), selected)
	defer cleanup()
	if got, ok := ctx.Deadline(); !ok || !got.Equal(deadline) {
		t.Fatal("callback deadline lost", got, ok)
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("callback deadline ignored")
	}
	if !errors.Is(context.Cause(ctx), cause) {
		t.Fatal(context.Cause(ctx))
	}
}
