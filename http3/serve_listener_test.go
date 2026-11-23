package http3

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
)

// A listener interface need not contain a comparable dynamic value.
type customHTTPListener struct {
	noncomparable []byte
	entered       chan struct{}
	closes        *atomic.Int32
	failure       error
}

func (l customHTTPListener) Addr() net.Addr { return &net.UDPAddr{} }
func (l customHTTPListener) Close() error   { l.closes.Add(1); return nil }
func (l customHTTPListener) Accept(ctx context.Context) (*quic.Conn, error) {
	close(l.entered)
	if l.failure != nil {
		return nil, l.failure
	}
	<-ctx.Done()
	return nil, context.Cause(ctx)
}
func TestServeListenerCustomOwnership(t *testing.T) {
	for _, mode := range []string{"close", "shutdown", "accept-error"} {
		t.Run(mode, func(t *testing.T) {
			var closes atomic.Int32
			listener := customHTTPListener{entered: make(chan struct{}), closes: &closes}
			want := ErrServerClosed
			if mode == "accept-error" {
				want = errors.New("listener failure")
				listener.failure = want
			}
			server := &Server{}
			result := make(chan error, 1)
			go func() { result <- server.ServeListener(listener) }()
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			select {
			case <-listener.entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			switch mode {
			case "close":
				server.Close()
			case "shutdown":
				if err := server.Shutdown(ctx); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case err := <-result:
				if !errors.Is(err, want) {
					t.Fatalf("got %v, want %v", err, want)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if err := server.Shutdown(ctx); err != nil {
				t.Fatal(err)
			}
			if closes.Load() != 0 {
				t.Fatal("server closed caller listener")
			}
		})
	}
}
