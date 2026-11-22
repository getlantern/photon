package quic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	engineStream "github.com/getlantern/photon/internal/stream"
)

func TestConfiguredIncomingStreamLimits(t *testing.T) {
	assertLimit := func(t *testing.T, err error) {
		t.Helper()
		var limit *StreamLimitReachedError
		if !errors.As(err, &limit) || !errors.Is(err, &StreamLimitReachedError{}) || !errors.Is(err, engineStream.ErrWouldBlock) {
			t.Fatalf("stream-limit error identity: %T %v", err, err)
		}
	}
	for _, version := range []Version{Version1, Version2} {
		for _, limits := range [][2]int64{{2, 3}, {-1, 2}, {2, -1}, {64, 1}, {1, 64}, {100, 100}, {300, 300}} {
			t.Run(fmt.Sprintf("%x/%d/%d", version, limits[0], limits[1]), func(t *testing.T) {
				clientTLS, serverTLS := dialTLS(t)
				cfg := &Config{Versions: []Version{version}, MaxIncomingStreams: limits[0], MaxIncomingUniStreams: limits[1]}
				l, err := ListenAddr("127.0.0.1:0", serverTLS, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer l.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				client, err := DialAddr(ctx, l.Addr().String(), clientTLS, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer client.server.Close()
				server, err := l.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				for _, peers := range [][2]*Conn{{client, server}, {server, client}} {
					sender, receiver := peers[0], peers[1]
					for i := int64(0); i < max(limits[0], 0); i++ {
						if _, err := sender.OpenStream(); err != nil {
							t.Fatalf("bidi %d: %v", i, err)
						}
					}
					if _, err := sender.OpenStream(); !errors.Is(err, engineStream.ErrWouldBlock) {
						t.Fatalf("bidi limit: %v", err)
					} else {
						assertLimit(t, err)
					}
					var first *SendStream
					for i := int64(0); i < max(limits[1], 0); i++ {
						s, err := sender.OpenUniStream()
						if err != nil {
							t.Fatalf("uni %d: %v", i, err)
						}
						if first == nil {
							first = s
						}
					}
					if _, err := sender.OpenUniStream(); !errors.Is(err, engineStream.ErrWouldBlock) {
						t.Fatalf("uni limit: %v", err)
					} else {
						assertLimit(t, err)
					}
					blocked, stop := context.WithTimeout(ctx, 10*time.Millisecond)
					_, err := sender.OpenUniStreamSync(blocked)
					stop()
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("blocked open: %v", err)
					}
					if first != nil {
						first.SetWriteDeadline(time.Now().Add(time.Second))
						if _, err := first.Write([]byte("credit")); err != nil {
							t.Fatal(err)
						}
						if err := first.Close(); err != nil {
							t.Fatal(err)
						}
						rx, err := receiver.AcceptUniStream(ctx)
						if err != nil {
							t.Fatal(err)
						}
						rx.SetReadDeadline(time.Now().Add(time.Second))
						if got, err := io.ReadAll(rx); err != nil || string(got) != "credit" {
							t.Fatalf("read %q: %v", got, err)
						}
						// Consuming the terminal stream must replenish the configured window.
						if _, err := sender.OpenUniStreamSync(ctx); err != nil {
							t.Fatalf("replenished credit: %v", err)
						}
						if _, err := sender.OpenUniStream(); !errors.Is(err, engineStream.ErrWouldBlock) {
							t.Fatalf("excess replenished credit: %v", err)
						} else {
							assertLimit(t, err)
						}
					}
				}
			})
		}
	}
}

func TestIncomingStreamLimitDefaults(t *testing.T) {
	cfg := &Config{}
	_, params, err := prepareConfig(cfg)
	if err != nil || params.InitialMaxStreamsBidi != 100 || params.InitialMaxStreamsUni != 100 {
		t.Fatalf("defaults: %+v, %v", params, err)
	}
	if cfg.MaxIncomingStreams != 0 || cfg.MaxIncomingUniStreams != 0 {
		t.Fatal("mutated caller config")
	}
}

func TestIncomingStreamLimitClipping(t *testing.T) {
	cfg := &Config{MaxIncomingStreams: 1 << 62, MaxIncomingUniStreams: 1 << 62}
	effective, p, err := prepareConfig(cfg)
	if err != nil || p.InitialMaxStreamsBidi != 1<<60 || p.InitialMaxStreamsUni != 1<<60 || effective.MaxIncomingStreams != 1<<60 || effective.MaxIncomingUniStreams != 1<<60 {
		t.Fatalf("clipping: %+v %v", p, err)
	}
	if cfg.MaxIncomingStreams != 1<<62 || cfg.MaxIncomingUniStreams != 1<<62 {
		t.Fatal("mutated caller")
	}
}
