package http3

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type poolTestBody struct {
	io.Reader
	closed atomic.Int32
}

func (b *poolTestBody) Close() error { b.closed.Add(1); return nil }

func TestTransportPreflightClosesBody(t *testing.T) {
	for _, tc := range []struct {
		name, url string
		transport *Transport
	}{
		{"scheme", "http://example.com", &Transport{}},
		{"limits", "https://example.com", &Transport{MaxConnections: -1}},
		{"timeout", "https://example.com", &Transport{DialTimeout: -1}},
		{"client", "https://example.com", &Transport{ClientConfig: &ClientConfig{MaxHeaderBytes: -1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &poolTestBody{Reader: strings.NewReader("hello")}
			request, _ := http.NewRequest("POST", tc.url, body)
			if response, err := tc.transport.RoundTrip(request); err == nil || response != nil {
				t.Fatal(response, err)
			}
			if body.closed.Load() != 1 || len(tc.transport.all) != 0 {
				t.Fatal("preflight ownership")
			}
		})
	}
}

func TestTransportSharedDialReservations(t *testing.T) {
	socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	transport := &Transport{MaxConnections: 1}
	defer transport.Close()
	first, err := transport.acquire(context.Background(), socket.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	second, err := transport.acquire(context.Background(), socket.LocalAddr().String())
	if err != nil || first != second {
		t.Fatal("dial not shared", err)
	}
	if _, err := transport.acquire(context.Background(), "localhost:443"); !errors.Is(err, ErrConnectionLimit) {
		t.Fatal(err)
	}
	transport.release(first)
	if second.ctx.Err() != nil {
		t.Fatal("one waiter canceled shared dial")
	}
	transport.release(second)
	select {
	case <-first.done:
	case <-time.After(time.Second):
		t.Fatal("abandoned dial not joined")
	}
	transport.mu.Lock()
	remaining := len(transport.all) + len(transport.pool)
	transport.mu.Unlock()
	if remaining != 0 {
		t.Fatal("abandoned dial retained")
	}
}

func TestTransportCloseInterruptsDial(t *testing.T) {
	socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	transport := new(Transport)
	entry, err := transport.acquire(context.Background(), socket.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	<-entry.ready
	if !errors.Is(entry.err, ErrTransportClosed) {
		t.Fatal(entry.err)
	}
	transport.release(entry)
	body := &poolTestBody{Reader: strings.NewReader("hello")}
	request, _ := http.NewRequest("POST", "https://example.com", body)
	if _, err := transport.RoundTrip(request); !errors.Is(err, ErrTransportClosed) {
		t.Fatal(err)
	}
	if body.closed.Load() != 1 {
		t.Fatal("body not closed")
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	transport.CloseIdleConnections()
}

func TestTransportDialTimeoutClosesRequestBody(t *testing.T) {
	socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	transport := &Transport{DialTimeout: 10 * time.Millisecond}
	defer transport.Close()
	body := &poolTestBody{Reader: strings.NewReader("hello")}
	request, _ := http.NewRequest("POST", "https://"+socket.LocalAddr().String(), body)
	if _, err := transport.RoundTrip(request); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if body.closed.Load() != 1 {
		t.Fatal("body ownership", body.closed.Load())
	}
	transport.CloseIdleConnections()
	transport.mu.Lock()
	remaining := len(transport.all) + len(transport.pool)
	transport.mu.Unlock()
	if remaining != 0 {
		t.Fatal("failed dial retained")
	}
}

func TestTransportEvictionWaitHonorsCancellation(t *testing.T) {
	makeEntry := func(origin string, refs int, idle time.Time) *pooledClient {
		ctx, cancel := context.WithCancelCause(context.Background())
		t.Cleanup(func() { cancel(context.Canceled) })
		return &pooledClient{origin: origin, ctx: ctx, cancel: cancel, refs: refs, idleSince: idle, ready: make(chan struct{}), done: make(chan struct{})}
	}
	oldest := makeEntry("oldest:443", 0, time.Now().Add(-time.Hour))
	recent := makeEntry("recent:443", 0, time.Now())
	active := makeEntry("active:443", 1, time.Time{})
	transport := &Transport{MaxConnections: 3, pool: map[string]*pooledClient{oldest.origin: oldest, recent.origin: recent, active.origin: active}, all: map[*pooledClient]struct{}{oldest: {}, recent: {}, active: {}}}
	deadline, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	ctx, cancel := context.WithCancelCause(deadline)
	defer cancel(context.Canceled)
	sentinel := errors.New("eviction wait canceled")
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-oldest.ctx.Done():
			cancel(sentinel)
		case <-ctx.Done():
		}
	}()
	entry, err := transport.acquire(ctx, "new:443")
	<-joined
	if entry != nil || !errors.Is(err, sentinel) {
		t.Fatal(entry, err)
	}
	if recent.ctx.Err() != nil || active.ctx.Err() != nil {
		t.Fatal("evicted active or more recent connection")
	}
	if !oldest.retired || len(transport.all) != 3 || len(transport.pool) != 2 {
		t.Fatal("closing connection lost capacity charge")
	}
}
