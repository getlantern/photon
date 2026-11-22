package http3

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
)

func TestTransportDialCallback(t *testing.T) {
	for _, name := range []string{"default", "explicit", "nil-result", "error"} {
		t.Run(name, func(t *testing.T) {
			sourceTLS := &tls.Config{NextProtos: []string{"original"}}
			sourceQUIC := &quic.Config{EnableDatagrams: true}
			if name == "explicit" {
				sourceTLS.ServerName = "certificate.example"
			}
			expected := errors.New("dial failure")
			calls := 0
			transport := &Transport{TLSClientConfig: sourceTLS, QUICConfig: sourceQUIC, EnableDatagrams: true,
				Dial: func(ctx context.Context, address string, tc *tls.Config, qc *quic.Config) (*quic.Conn, error) {
					calls++
					wantName := "example.com"
					if name == "explicit" {
						wantName = sourceTLS.ServerName
					}
					if address != "example.com:443" || tc == sourceTLS || qc == sourceQUIC || tc.ServerName != wantName || len(tc.NextProtos) != 1 || tc.NextProtos[0] != "h3" || !qc.EnableDatagrams {
						return nil, fmt.Errorf("incorrect callback configuration: %s %v %v", address, tc, qc)
					}
					tc.ServerName = "callback.example"
					qc.EnableDatagrams = false
					if name == "nil-result" {
						return nil, nil
					}
					return nil, expected
				}}
			defer transport.Close()
			request, _ := http.NewRequest("GET", "https://example.com/", nil)
			_, err := transport.RoundTrip(request)
			transport.Close()
			if name == "nil-result" {
				if err == nil {
					t.Fatal("nil connection accepted")
				}
			} else if !errors.Is(err, expected) {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatal("dial calls", calls)
			}
			if sourceTLS.NextProtos[0] != "original" || sourceTLS.ServerName == "callback.example" || !sourceQUIC.EnableDatagrams {
				t.Fatal("caller configuration mutated")
			}
		})
	}
}

func TestTransportDialCancellation(t *testing.T) {
	for _, mode := range []string{"request", "close", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			entered := make(chan struct{})
			exited := make(chan struct{})
			transport := &Transport{Dial: func(ctx context.Context, _ string, _ *tls.Config, _ *quic.Config) (*quic.Conn, error) {
				close(entered)
				defer close(exited)
				<-ctx.Done()
				return nil, context.Cause(ctx)
			}}
			if mode == "timeout" {
				transport.DialTimeout = 20 * time.Millisecond
			}
			defer transport.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			request, _ := http.NewRequestWithContext(ctx, "GET", "https://example.com/", nil)
			done := make(chan error, 1)
			go func() { _, err := transport.RoundTrip(request); done <- err }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("dial not entered")
			}
			switch mode {
			case "request":
				cancel()
			case "close":
				transport.Close()
			}
			select {
			case err := <-done:
				want := error(context.Canceled)
				if mode == "close" {
					want = ErrTransportClosed
				}
				if mode == "timeout" {
					want = context.DeadlineExceeded
				}
				if !errors.Is(err, want) {
					t.Fatalf("got %v, want %v", err, want)
				}
			case <-time.After(time.Second):
				t.Fatal("request did not exit")
			}
			select {
			case <-exited:
			case <-time.After(time.Second):
				t.Fatal("dialer not canceled")
			}
		})
	}
}
