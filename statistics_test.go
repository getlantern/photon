package quic

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

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
