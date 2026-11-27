package quic

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/transport"
)

func TestConnectionStatisticsTrafficAndShutdown(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			clientTLS, serverTLS := dialTLS(t)
			listener, err := ListenAddr("127.0.0.1:0", serverTLS, &Config{Versions: []Version{version}})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := DialAddr(ctx, listener.Addr().String(), clientTLS, &Config{Versions: []Version{version}})
			if err != nil {
				t.Fatal(err)
			}
			defer client.server.Close()
			server, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			stop := make(chan struct{})
			var readers sync.WaitGroup
			for i := 0; i < 4; i++ {
				readers.Go(func() {
					for {
						select {
						case <-stop:
							return
						default:
						}
						_ = client.ConnectionStats()
						_ = server.ConnectionStats()
					}
				})
			}
			defer func() { close(stop); readers.Wait() }()
			// A getter invoked on the owner must not enqueue a command to itself.
			if err := client.server.Do(ctx, func(*transport.ServerEndpoint) error { _ = client.ConnectionStats(); return nil }); err != nil {
				t.Fatal(err)
			}
			payload := bytes.Repeat([]byte("statistics"), 4096)
			sent := make(chan error, 1)
			go func() {
				s, err := client.OpenUniStreamSync(ctx)
				if err == nil {
					s.SetWriteDeadline(time.Now().Add(4 * time.Second))
					_, err = s.Write(payload)
					if err == nil {
						err = s.Close()
					}
				}
				sent <- err
			}()
			rx, err := server.AcceptUniStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			rx.SetReadDeadline(time.Now().Add(4 * time.Second))
			got, err := io.ReadAll(rx)
			if err != nil || !bytes.Equal(got, payload) {
				t.Fatal("receive", len(got), err)
			}
			if err := <-sent; err != nil {
				t.Fatal(err)
			}
			for _, c := range []*Conn{client, server} {
				s := c.ConnectionStats()
				if s.PacketsSent == 0 || s.PacketsReceived == 0 || s.BytesSent == 0 || s.BytesReceived == 0 || s.SmoothedRTT <= 0 {
					t.Fatal("empty snapshot", s)
				}
			}
			if client.ConnectionStats().BytesSent < uint64(len(payload)) || server.ConnectionStats().BytesReceived < uint64(len(payload)) {
				t.Fatal("payload missing from totals")
			}
			listener.Close()
			client.server.Close()
			for _, c := range []*Conn{client, server} {
				final := c.ConnectionStats()
				if final.BytesSent == 0 || final != c.ConnectionStats() {
					t.Fatal("final snapshot lost", final)
				}
				copy := final
				copy.BytesSent = 0
				if c.ConnectionStats() != final {
					t.Fatal("caller changed snapshot")
				}
			}
		})
	}
}

func TestConnectionStatisticsCongestionWindow(t *testing.T) {
	for _, tc := range []struct {
		version    Version
		congestion CongestionControl
	}{{Version1, CongestionControlBBR}, {Version2, CongestionControlBBR}, {Version1, CongestionControlNewReno}, {Version2, CongestionControlNewReno}} {
		version := tc.version
		t.Run(fmt.Sprintf("%x/%d", version, tc.congestion), func(t *testing.T) {
			clientTLS, serverTLS := dialTLS(t)
			config := &Config{Versions: []Version{version}, CongestionControl: tc.congestion}
			listener, err := ListenAddr("127.0.0.1:0", serverTLS, config)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := DialAddr(ctx, listener.Addr().String(), clientTLS, config)
			if err != nil {
				t.Fatal(err)
			}
			defer client.server.Close()
			server, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			// The window changes only inside network and timer events, each of
			// which republishes, so on the owner the snapshot equals the engine.
			window := func(c *Conn) uint64 {
				t.Helper()
				var published uint64
				if err := c.server.Do(ctx, func(*transport.ServerEndpoint) error {
					published = c.ConnectionStats().CongestionWindow
					if engine, _ := c.raw.CongestionStats(); published != engine {
						return fmt.Errorf("published window %d, engine %d", published, engine)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				return published
			}
			// BBR's Startup grows the window with every acknowledgment, even
			// of the receiver's own few control packets, so the receiver's
			// window is compared only once its packets have all been
			// acknowledged.
			drained := func(c *Conn) {
				t.Helper()
				for {
					var flight uint64
					if err := c.server.Do(ctx, func(*transport.ServerEndpoint) error {
						_, flight = c.raw.CongestionStats()
						return nil
					}); err != nil {
						t.Fatal(err)
					}
					if flight == 0 {
						return
					}
					time.Sleep(time.Millisecond)
				}
			}
			// The server's post-handshake packets (HANDSHAKE_DONE, NEW_TOKEN,
			// NEW_CONNECTION_ID) and the client's last handshake packets.
			drained(server)
			drained(client)
			initial, idle := window(client), window(server)
			// The initial window bounds NewReno's for any datagram size. BBR's
			// Startup also grows its window with the handshake's acknowledged
			// bytes, but never below its minimum.
			low, high := uint64(2*1200), recovery.InitialWindow(1452)
			if tc.congestion == CongestionControlBBR {
				low, high = 4*1200, 64*1452
			}
			if initial < low || initial > high || idle < low || idle > high {
				t.Fatal("initial windows", initial, idle)
			}
			payload := bytes.Repeat([]byte("statistics"), 20000)
			sent := make(chan error, 1)
			go func() {
				s, err := client.OpenUniStreamSync(ctx)
				if err == nil {
					s.SetWriteDeadline(time.Now().Add(4 * time.Second))
					if _, err = s.Write(payload); err == nil {
						if err = s.Close(); err == nil {
							err = s.WaitAcknowledged(ctx)
						}
					}
				}
				sent <- err
			}()
			rx, err := server.AcceptUniStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			rx.SetReadDeadline(time.Now().Add(4 * time.Second))
			if got, err := io.ReadAll(rx); err != nil || !bytes.Equal(got, payload) {
				t.Fatal("receive", len(got), err)
			}
			if err := <-sent; err != nil {
				t.Fatal(err)
			}
			// NewReno's slow start grows the sender's window. (BBR's is bounded
			// by its estimate of the path, which on loopback can be smaller
			// than the window it started with.)
			grown := window(client)
			if tc.congestion == CongestionControlNewReno && grown <= initial {
				t.Fatal("sender window did not grow", initial, grown)
			}
			if grown < low {
				t.Fatal("sender window below the minimum", grown)
			}
			// The receiver sent only ACKs and the MAX_STREAMS that released the
			// stream; NewReno doesn't grow while application limited.
			drained(server)
			if got := window(server); tc.congestion == CongestionControlNewReno && got != idle {
				t.Fatal("receiver window changed", idle, got)
			}
			final := client.ConnectionStats().CongestionWindow
			listener.Close()
			client.server.Close()
			if got := client.ConnectionStats().CongestionWindow; got != final || got == 0 {
				t.Fatal("final window lost", final, got)
			}
		})
	}
}
