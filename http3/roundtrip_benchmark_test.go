package http3

import (
	"context"
	"crypto/tls"
	"io"
	"net/http"
	"testing"
	"time"
)

// BenchmarkRoundTrip measures one small request and response on an established
// connection: the path every request shares, including QPACK feedback.
func BenchmarkRoundTrip(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	server := &Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Benchmark", "response header value")
		io.WriteString(w, "ok")
	})}
	origin := "https://" + listenBenchmarkServer(ctx, b, server)
	client, err := Dial(ctx, origin, insecureTLS(), nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		request, _ := http.NewRequestWithContext(ctx, "GET", origin+"/benchmark", nil)
		request.Header.Set("X-Benchmark", "request header value")
		response, err := client.RoundTrip(request)
		if err != nil {
			b.Fatal(err)
		}
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
	}
}

func insecureTLS() *tls.Config { return &tls.Config{InsecureSkipVerify: true} }

func listenBenchmarkServer(ctx context.Context, b *testing.B, server *Server) string {
	b.Helper()
	server.Addr, server.TLSConfig = "127.0.0.1:0", testServerTLS()
	served := make(chan error, 1)
	go func() { served <- server.ListenAndServe() }()
	b.Cleanup(func() { server.Close() })
	for {
		server.mu.Lock()
		for entry := range server.listeners {
			server.mu.Unlock()
			return entry.listener.Addr().String()
		}
		server.mu.Unlock()
		select {
		case err := <-served:
			b.Fatal("startup failed", err)
		case <-ctx.Done():
			b.Fatal(ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
}
