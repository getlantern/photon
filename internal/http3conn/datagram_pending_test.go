package http3conn

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestPendingDatagramSemantics(t *testing.T) {
	for _, mode := range []string{"supported", "unsupported", "oversized", "closed"} {
		t.Run(mode, func(t *testing.T) {
			d, _ := NewDatagramDispatch(1, 2, 8)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r, err := d.register(0, false, true, ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Remove()
			payload := []byte{0, 9}
			if mode == "oversized" {
				payload = make([]byte, 10)
			}
			if err := d.Deliver(payload); err != nil {
				t.Fatal("premature semantics error", err)
			}
			payload[len(payload)-1] = 42
			if _, err := r.Receive(); err != ErrNoHTTPDatagram {
				t.Fatal("pending datagram delivered", err)
			}
			if err := d.Deliver([]byte{0, 10}); err != nil {
				t.Fatal(err)
			}
			if err := d.Deliver([]byte{0, 11}); err != nil {
				t.Fatal(err)
			}
			if len(r.queue) > 2 {
				t.Fatal("pending queue unbounded")
			}
			if mode == "closed" {
				cancel()
			}
			err = r.resolve(mode == "supported")
			if mode == "unsupported" || mode == "oversized" {
				var unsupported *DatagramRequestError
				if !errors.As(err, &unsupported) || unsupported.ID != 0 {
					t.Fatal(err)
				}
				if _, err := r.Receive(); err != ErrDatagramRequestClosed {
					t.Fatal(err)
				}
			} else if mode == "closed" {
				if err != nil {
					t.Fatal("closed stream was aborted", err)
				}
				if _, err := r.Receive(); err != ErrDatagramRequestClosed {
					t.Fatal(err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				for _, want := range []byte{9, 10} {
					data, err := r.Receive()
					if err != nil || !bytes.Equal(data, []byte{want}) {
						t.Fatal(data, err)
					}
				}
				if _, err := r.Receive(); err != ErrNoHTTPDatagram {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestDatagramAbortCancelsServerHandler(t *testing.T) {
	old, raw, session := boundDatagramFixture(t, bytes.NewReader(readerHeaders(t, requestFields("/")...)))
	old.DetachDatagrams()
	raw.id = 4
	bound, err := session.BindPendingDatagramStream(raw)
	if err != nil {
		t.Fatal(err)
	}
	defer bound.DetachDatagrams()
	entered := make(chan struct{})
	config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done() }))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- ServeRequest(ctx, bound, config) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	err = session.datagrams.Deliver([]byte{1, 9})
	var unsupported *DatagramRequestError
	if !errors.As(err, &unsupported) {
		t.Fatal(err)
	}
	if err := session.datagrams.AbortRequest(ctx, 4, unsupported.Code()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.As(err, &unsupported) || unsupported.ID != 4 {
			t.Fatal("handler lost datagram cause", err)
		}
	case <-ctx.Done():
		t.Fatal("handler ignored datagram abort", ctx.Err())
	}
}
