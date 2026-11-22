package quic

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/transport"
)

func TestReceiveWindowPolicyPublicBinding(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		for _, serverRole := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/server=%t", version, serverRole), func(t *testing.T) {
				var expected atomic.Pointer[Conn]
				calls := make(chan uint64, 4)
				cfg := &Config{Versions: []Version{version}, InitialStreamReceiveWindow: 1024, InitialConnectionReceiveWindow: 1024, MaxConnectionReceiveWindow: 2048,
					AllowConnectionWindowIncrease: func(c *Conn, delta uint64) bool {
						if c != expected.Load() {
							t.Error("policy received wrong public connection")
						}
						calls <- delta
						return false
					}}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				ct, st := dialTLS(t)
				serverCfg, clientCfg := (*Config)(nil), (*Config)(nil)
				if serverRole {
					serverCfg = &Config{GetConfigForClient: func(*ClientInfo) (*Config, error) { return cfg, nil }}
				} else {
					clientCfg = cfg
				}
				if clientCfg == nil {
					clientCfg = &Config{Versions: []Version{version}}
				}
				listener, err := ListenAddr("127.0.0.1:0", st, serverCfg)
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				client, err := DialAddr(ctx, listener.Addr().String(), ct, clientCfg)
				if err != nil {
					t.Fatal(err)
				}
				defer client.CloseWithError(0, "")
				server, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				receiver, sender := client, server
				if serverRole {
					receiver, sender = server, client
				}
				expected.Store(receiver)
				tx, err := sender.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Write(make([]byte, 1024)); err != nil {
					t.Fatal(err)
				}
				if err := tx.Close(); err != nil {
					t.Fatal(err)
				}
				rx, err := receiver.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err := tx.WaitAcknowledged(ctx); err != nil {
					t.Fatal(err)
				}
				// Drive reads on the real owner with controlled timestamps so callback
				// coverage does not depend on localhost RTT and goroutine scheduling.
				err = receiver.server.Do(ctx, func(*transport.ServerEndpoint) error {
					if !receiver.raw.RTTStats().Sampled {
						return fmt.Errorf("fixture lacks measured RTT")
					}
					at := time.Now()
					for _, size := range []int{1, 512} {
						n, err := receiver.raw.ReadStreamAt(uint64(rx.StreamID()), make([]byte, size), at)
						if err != nil || n != size {
							return fmt.Errorf("read %d: %d %v", size, n, err)
						}
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				select {
				case delta := <-calls:
					if delta != 1024 {
						t.Fatal("wrong additional credit", delta)
					}
				case <-ctx.Done():
					t.Fatal("policy not installed", ctx.Err())
				}
				select {
				case <-calls:
					t.Fatal("unexpected extra policy call")
				default:
				}
			})
		}
	}
}
