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
