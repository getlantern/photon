package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"
)

func TestListenConfiguration(t *testing.T) {
	// The certificate is never used: these tests only validate configuration
	// and listener lifetime without admitting peers.
	serverTLS := &tls.Config{NextProtos: []string{"test"}, Certificates: []tls.Certificate{{}}}
	for _, cfg := range []Config{
		{Versions: []Version{Version(99)}},
		{Versions: []Version{Version1, Version1}},
		{Versions: []Version{Version1, Version2, Version1}},
		{HandshakeTimeout: -time.Second},
		{MaxIdleTimeout: -time.Second},
		{KeepAlivePeriod: -time.Second},
		{MaxConnections: -1},
		{MaxConnections: 65537},
	} {
		if l, err := ListenAddr("127.0.0.1:0", serverTLS, &cfg); err == nil {
			l.Close()
			t.Fatalf("accepted invalid config: %+v", cfg)
		}
	}
	for _, cfg := range []*tls.Config{nil, {}, {NextProtos: []string{"test"}}, {Certificates: serverTLS.Certificates}, {Certificates: serverTLS.Certificates, NextProtos: serverTLS.NextProtos, MaxVersion: tls.VersionTLS12}} {
		if l, err := ListenAddr("127.0.0.1:0", cfg, nil); err == nil {
			l.Close()
			t.Fatal("accepted invalid TLS config")
		}
	}
	l, err := ListenAddr("127.0.0.1:0", serverTLS, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := l.Accept(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled accept", err)
	}
	result := make(chan error, 1)
	go func() { _, err := l.Accept(context.Background()); result <- err }()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("accept succeeded after close")
		}
	case <-time.After(time.Second):
		t.Fatal("accept blocked after close")
	}
}

// Closing a listener that owns its socket first sends what is already queued,
// so a connection closed just before still tells its peer instead of leaving
// it to the idle timeout. Repeated, because without that the close loses a race.
func TestListenerCloseSendsQueuedConnectionClose(t *testing.T) {
	for i := 0; i < 20; i++ {
		clientTLS, serverTLS := dialTLS(t)
		l, err := ListenAddr("127.0.0.1:0", serverTLS, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { l.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		t.Cleanup(cancel)
		client, err := DialAddr(ctx, l.Addr().String(), clientTLS, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { client.server.Close() })
		server, err := l.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		server.CloseWithError(0x42, "closing")
		l.Close()
		select {
		case <-client.Context().Done():
		case <-time.After(3 * time.Second): // far below the idle timeout
			t.Fatal("the client never heard the close, run", i)
		}
		var closed *ApplicationError
		if cause := context.Cause(client.Context()); !errors.As(cause, &closed) || closed.ErrorCode != 0x42 {
			t.Fatal("close cause:", cause)
		}
		client.server.Close()
		cancel()
	}
}
