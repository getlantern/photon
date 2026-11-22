package http3

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestServerReplayPolicy(t *testing.T) {
	for _, forwarded := range []bool{false, true} {
		for _, complete := range []bool{false, true} {
			for _, allow := range []bool{false, true} {
				request := httptest.NewRequest(http.MethodGet, "https://example.com/", nil)
				request.TLS = &tls.ConnectionState{HandshakeComplete: complete}
				if forwarded {
					request.Header.Set("Early-Data", "1")
				}
				handshake := make(chan struct{})
				observed := make(chan bool, 1)
				checked := make(chan struct{}, 1)
				handler := replayHandler(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { observed <- r.TLS.HandshakeComplete }), func(*http.Request) bool { checked <- struct{}{}; return allow }, handshake, func() tls.ConnectionState { return tls.ConnectionState{HandshakeComplete: true} })
				response := httptest.NewRecorder()
				done := make(chan struct{})
				go func() { defer close(done); handler.ServeHTTP(response, request) }()
				if !complete && !forwarded && !allow {
					select {
					case <-checked:
					case <-time.After(time.Second):
						t.Fatal("policy not consulted")
					}
					select {
					case <-observed:
						t.Fatal("resource ran before TLS")
					default:
					}
					close(handshake)
				}
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("policy did not finish", forwarded, complete, allow)
				}
				if forwarded && !allow {
					if response.Code != http.StatusTooEarly {
						t.Fatal("forwarded early request not rejected", response.Code)
					}
					select {
					case <-observed:
						t.Fatal("rejected request reached resource")
					default:
					}
				} else {
					select {
					case state := <-observed:
						if state != (complete || !allow) {
							t.Fatal("wrong dispatch TLS state", state)
						}
					default:
						t.Fatal("allowed request not dispatched")
					}
				}
			}
		}
	}
}

func TestServerDefaultReplayPolicyAndCancellation(t *testing.T) {
	var called atomic.Int32
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called.Add(1) })
	handshake := make(chan struct{})
	current := func() tls.ConnectionState { return tls.ConnectionState{HandshakeComplete: true} }
	handler := replayHandler(next, nil, handshake, current)
	forwarded := httptest.NewRequest(http.MethodGet, "https://example.com/", nil)
	forwarded.Header.Set("Early-Data", "1")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, forwarded)
	if response.Code != http.StatusTooEarly || called.Load() != 0 {
		t.Fatal("unsafe default forwarded policy")
	}
	ctx, cancel := context.WithCancel(context.Background())
	direct := httptest.NewRequest(http.MethodGet, "https://example.com/", nil).WithContext(ctx)
	done := make(chan struct{})
	go func() { defer close(done); handler.ServeHTTP(httptest.NewRecorder(), direct) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("TLS wait ignored cancellation")
	}
	if called.Load() != 0 {
		t.Fatal("canceled request dispatched")
	}
	close(handshake)
	direct = httptest.NewRequest(http.MethodGet, "https://example.com/", nil)
	handler.ServeHTTP(httptest.NewRecorder(), direct)
	if called.Load() != 1 || direct.TLS == nil || !direct.TLS.HandshakeComplete {
		t.Fatal("default did not refresh TLS after wait")
	}
}
