package interop

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"testing"
	"time"

	upstream "github.com/quic-go/quic-go"

	quic "github.com/getlantern/photon"
)

// resumeServer is a photon server that answers each stream's 8-byte length
// with that many bytes, its listener keeping congestion state in cache.
func resumeServer(tb testing.TB, ctx context.Context, serverTLS *tls.Config, cache quic.CongestionStateCache) string {
	tb.Helper()
	const window = 32 << 20
	l, err := quic.ListenAddr("127.0.0.1:0", serverTLS, &quic.Config{Versions: []quic.Version{quic.Version1}, InitialStreamReceiveWindow: window / 2, MaxStreamReceiveWindow: window / 2, InitialConnectionReceiveWindow: window, MaxConnectionReceiveWindow: window, CongestionStateCache: cache})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept(ctx)
			if err != nil {
				return
			}
			go func() {
				for {
					s, err := c.AcceptStream(ctx)
					if err != nil {
						return
					}
					go func() {
						var n [8]byte
						if _, err := io.ReadFull(s, n[:]); err != nil {
							return
						}
						s.Write(make([]byte, binary.BigEndian.Uint64(n[:])))
						s.Close()
					}()
				}
			}()
		}
	}()
	return l.Addr().String()
}

// resumeDownload connects to server through a fresh relay applying link,
// downloads n bytes, closes the connection and reports how long the
// download took. during, if set, runs once the download has started.
func resumeDownload(tb testing.TB, ctx context.Context, server string, clientTLS *tls.Config, link wanLink, n uint64, during func()) time.Duration {
	tb.Helper()
	relay := newWANRelay(tb, server, link, link)
	dialCtx, stop := context.WithTimeout(ctx, 30*time.Second)
	defer stop()
	const window = 32 << 20
	peer, err := dialUpstreamAddr(tb, dialCtx, relay.addr(), clientTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version1}, InitialStreamReceiveWindow: window / 2, MaxStreamReceiveWindow: window / 2, InitialConnectionReceiveWindow: window, MaxConnectionReceiveWindow: window})
	if err != nil {
		tb.Fatal(err)
	}
	defer peer.CloseWithError(0, "")
	start := time.Now()
	s, err := peer.OpenStreamSync(ctx)
	if err != nil {
		tb.Fatal(err)
	}
	var req [8]byte
	binary.BigEndian.PutUint64(req[:], n)
	if _, err := s.Write(req[:]); err != nil {
		tb.Fatal(err)
	}
	s.Close()
	if during != nil {
		var first [1]byte
		if _, err := io.ReadFull(s, first[:]); err != nil {
			tb.Fatal(err)
		}
		n--
		during()
	}
	got, err := io.Copy(io.Discard, s)
	if err != nil || uint64(got) != n {
		tb.Fatal("download", got, "of", n, err)
	}
	return time.Since(start)
}

// waitForStates waits until cache holds n states: a server connection saves
// its state once it has drained after the client closes.
func waitForStates(tb testing.TB, cache *quic.MemoryCongestionStateCache, n int) {
	tb.Helper()
	for deadline := time.Now().Add(10 * time.Second); cache.Len() != n; {
		if time.Now().After(deadline) {
			tb.Fatalf("cache holds %d states, want %d", cache.Len(), n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A server connection saves its path's state at close, keyed by the
// client's address; the client's next connection takes it, so the cache
// is empty while that connection runs, and saves its own at close.
func TestCarefulResumeSavesAndResumes(t *testing.T) {
	clientTLS, serverTLS := tlsConfigs(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache := quic.NewMemoryCongestionStateCache(0, 0)
	server := resumeServer(t, ctx, serverTLS, cache)
	link := wanLink{delay: 50 * time.Millisecond, rate: 20e6 / 8, queue: 100 * time.Millisecond}
	resumeDownload(t, ctx, server, clientTLS, link, 4<<20, nil)
	waitForStates(t, cache, 1)
	s, ok := cache.Take("127.0.0.1")
	if !ok || s.Bandwidth < 1e6 || s.Bandwidth > 4e6 || s.MinRTT < 100*time.Millisecond || s.MinRTT > 200*time.Millisecond {
		t.Fatalf("saved %+v, %v after 4 MB through a 20 Mbit/s, 100 ms relay", s, ok)
	}
	cache.Put("127.0.0.1", s)
	resumeDownload(t, ctx, server, clientTLS, link, 4<<20, func() {
		if cache.Len() != 0 {
			t.Error("the second connection didn't take the saved state")
		}
	})
	waitForStates(t, cache, 1)
}

// BenchmarkCarefulResume times a returning client's download, after one
// warming connection to the same server, with and without Careful Resume.
// Run with -benchtime=1x.
func BenchmarkCarefulResume(b *testing.B) {
	for _, tc := range []struct {
		name string
		rtt  time.Duration
		rate float64
		n    uint64
	}{{"rtt50ms-50mbit-4MiB", 50 * time.Millisecond, 50e6 / 8, 4 << 20}, {"rtt150ms-50mbit-8MiB", 150 * time.Millisecond, 50e6 / 8, 8 << 20}, {"rtt300ms-50mbit-8MiB", 300 * time.Millisecond, 50e6 / 8, 8 << 20}} {
		for _, resume := range []bool{false, true} {
			name := tc.name + "/fresh"
			if resume {
				name = tc.name + "/resumed"
			}
			b.Run(name, func(b *testing.B) {
				clientTLS, serverTLS := tlsConfigs(b)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				cache := quic.NewMemoryCongestionStateCache(0, 0)
				server := resumeServer(b, ctx, serverTLS, cache)
				link := wanLink{delay: tc.rtt / 2, rate: tc.rate, queue: tc.rtt}
				resumeDownload(b, ctx, server, clientTLS, link, 16<<20, nil)
				waitForStates(b, cache, 1)
				if !resume {
					cache.Flush()
				}
				b.SetBytes(int64(tc.n))
				b.ResetTimer()
				d := resumeDownload(b, ctx, server, clientTLS, link, tc.n, nil)
				b.StopTimer()
				b.ReportMetric(float64(tc.n)/d.Seconds()/1e6, "MB/s")
			})
		}
	}
}
