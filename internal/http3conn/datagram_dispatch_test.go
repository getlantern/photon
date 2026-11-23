package http3conn

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	h3 "github.com/getlantern/photon/internal/http3wire"
)

func TestDatagramDispatchLimitsAndLifetime(t *testing.T) {
	d, err := NewDatagramDispatch(2, 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	r, err := d.Register(0, true)
	if err != nil {
		t.Fatal(err)
	}
	unsupported, _ := d.Register(4, false)
	if _, err := d.Register(8, true); !errors.Is(err, ErrRequestLimit) {
		t.Fatal(err)
	}
	if _, err := d.Register(1, true); !errors.Is(err, ErrRequestID) {
		t.Fatal(err)
	}
	data := []byte{0, 1, 2, 3}
	if err := d.Deliver(data); err != nil {
		t.Fatal(err)
	}
	data[1] = 9
	d.Deliver([]byte{0})    // Empty datagram occupies a queue slot.
	d.Deliver([]byte{0, 4}) // Full queue drops newest.
	first, err := r.Receive()
	if err != nil || !bytes.Equal(first, []byte{1, 2, 3}) {
		t.Fatal(first, err)
	}
	empty, err := r.Receive()
	if err != nil || len(empty) != 0 {
		t.Fatal(empty, err)
	}
	if _, err := r.Receive(); !errors.Is(err, ErrNoHTTPDatagram) {
		t.Fatal(err)
	}
	d.Deliver([]byte{0, 1, 2, 3, 4}) // Oversized payload drops.
	if _, err := r.Receive(); !errors.Is(err, ErrNoHTTPDatagram) {
		t.Fatal(err)
	}
	if err := d.Deliver([]byte{2, 9}); err != nil {
		t.Fatal("unopened stream", err)
	}
	var request *DatagramRequestError
	if err := d.Deliver([]byte{1, 9}); !errors.As(err, &request) || request.ID != 4 || request.Code() != h3.DatagramError {
		t.Fatal(err)
	}
	if err := d.Deliver([]byte{1, 9}); err != nil {
		t.Fatal("closed request should drop", err)
	}
	unsupported.Remove()
	if _, err := d.Register(8, true); err != nil {
		t.Fatal("budget not released", err)
	}
	r.CloseReceive()
	if err := d.Deliver([]byte{0, 9}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Receive(); !errors.Is(err, ErrDatagramRequestClosed) {
		t.Fatal(err)
	}
	// Bad encoding is a connection error even when no request exists.
	var protocol *h3.Error
	if err := d.Deliver(nil); !errors.As(err, &protocol) || protocol.Code != h3.DatagramError {
		t.Fatal(err)
	}
}

type datagramSource struct{ input chan []byte }

func (s datagramSource) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	select {
	case data := <-s.input:
		return data, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func TestDatagramDispatchLoopErrorScopes(t *testing.T) {
	d, _ := NewDatagramDispatch(2, 2, 8)
	unsupported, _ := d.Register(0, false)
	supported, _ := d.Register(4, true)
	source := datagramSource{input: make(chan []byte, 4)}
	source.input <- []byte{0, 7}
	source.input <- []byte{1, 8}
	source.input <- []byte{0, 7} // No second request abort after closure.
	source.input <- nil          // Fatal malformed datagram.
	aborted := 0
	err := d.Run(context.Background(), source, func(_ context.Context, id, code uint64) error {
		if id != 0 || code != h3.DatagramError {
			t.Fatal(id, code)
		}
		aborted++
		unsupported.Remove() // Callback can safely enter the registry.
		return nil
	})
	var protocol *h3.Error
	if !errors.As(err, &protocol) || protocol.Code != h3.DatagramError || aborted != 1 {
		t.Fatal(err, aborted)
	}
	payload, err := supported.Receive()
	if err != nil || !bytes.Equal(payload, []byte{8}) {
		t.Fatal(payload, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := d.Run(ctx, source, func(context.Context, uint64, uint64) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
func TestDatagramDispatchConcurrentClose(t *testing.T) {
	d, _ := NewDatagramDispatch(1, 4, 16)
	r, _ := d.Register(0, true)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Go(func() {
			for j := 0; j < 100; j++ {
				d.Deliver([]byte{0, 7})
				r.Receive()
			}
		})
	}
	wg.Go(func() { r.CloseReceive(); r.Remove() })
	wg.Wait()
	if _, err := r.Receive(); !errors.Is(err, ErrDatagramRequestClosed) {
		t.Fatal(err)
	}
}

func TestDatagramReceiveWakeups(t *testing.T) {
	for _, mode := range []string{"data", "request-close", "remove", "dispatch-close", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			d, _ := NewDatagramDispatch(1, 2, 8)
			r, _ := d.Register(0, true)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				data, err := r.ReceiveContext(ctx)
				if err == nil && !bytes.Equal(data, []byte{9}) {
					err = errors.New("wrong payload")
				}
				result <- err
			}()
			// Observe actual waiter installation before triggering the wakeup.
			for {
				d.mu.Lock()
				waiting := r.changed != nil
				d.mu.Unlock()
				if waiting {
					break
				}
				if ctx.Err() != nil {
					t.Fatal("receiver never waited")
				}
				runtime.Gosched()
			}
			switch mode {
			case "data":
				d.Deliver([]byte{0, 9})
			case "request-close":
				r.CloseReceive()
			case "remove":
				r.Remove()
			case "dispatch-close":
				d.Close()
				d.Close()
			case "cancel":
				cancel()
			}
			err := <-result
			if mode == "data" {
				if err != nil {
					t.Fatal(err)
				}
			} else if mode == "cancel" {
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrDatagramRequestClosed) {
				t.Fatal(err)
			}
			if mode == "dispatch-close" {
				if _, err := d.Register(4, true); !errors.Is(err, ErrDatagramRequestClosed) {
					t.Fatal("registered after close", err)
				}
			}
		})
	}
}

func TestDatagramCanceledWaitPreservesPayload(t *testing.T) {
	d, _ := NewDatagramDispatch(1, 2, 8)
	r, _ := d.Register(0, true)
	d.Deliver([]byte{0, 9})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.ReceiveContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	data, err := r.ReceiveContext(context.Background())
	if err != nil || !bytes.Equal(data, []byte{9}) {
		t.Fatal("canceled waiter consumed payload", data, err)
	}
}
