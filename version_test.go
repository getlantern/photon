package quic

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"
	"time"
)

func TestSupportedVersionsOwnership(t *testing.T) {
	first := SupportedVersions()
	if len(first) != 2 || first[0] != Version1 || first[1] != Version2 {
		t.Fatal(first)
	}
	first[0] = 0
	second := SupportedVersions()
	if second[0] != Version1 {
		t.Fatal("shared version defaults")
	}
}

func TestVersionTLSContext(t *testing.T) {
	for _, original := range SupportedVersions() {
		for _, compatible := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/compatible=%v", original, compatible), func(t *testing.T) {
				ct, st := dialTLS(t)
				seen := make(chan context.Context, 1)
				st.GetConfigForClient = func(h *tls.ClientHelloInfo) (*tls.Config, error) { seen <- h.Context(); return nil, nil }
				cfg := &Config{Versions: []Version{original}}
				selected := original
				if compatible {
					if original == Version1 {
						selected = Version2
					} else {
						selected = Version1
					}
					cfg = &Config{Versions: []Version{selected, original}, InitialVersion: original, EnableCompatibleVersionNegotiation: true}
				}
				socket := helperSocket(t)
				tr := &Transport{Conn: socket, ConnContext: func(ctx context.Context, _ *ClientInfo) (context.Context, error) {
					// Application metadata survives, but the wire annotation supersedes any
					// value the parent supplied under the public key.
					return context.WithValue(context.WithValue(ctx, transportContextKey{}, "metadata"), QUICVersionContextKey, Version(0)), nil
				}}
				defer tr.Close()
				l, err := tr.Listen(st, cfg)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				c, err := DialAddr(ctx, l.Addr().String(), ct, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer c.server.Close()
				s, err := l.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				var hello context.Context
				select {
				case hello = <-seen:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if got := hello.Value(QUICVersionContextKey); got != original {
					t.Fatalf("TLS version: %v, want %v", got, original)
				}
				if hello.Value(transportContextKey{}) != "metadata" {
					t.Fatal("lost application context")
				}
				if c.ConnectionState().Version != selected || s.ConnectionState().Version != selected {
					t.Fatal("wrong negotiated version")
				}
			})
		}
	}
}
