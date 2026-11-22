package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/wire"
)

func TestMigrationCancellationCommitRace(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, delay := range []time.Duration{-1, 0, 50 * time.Microsecond, 200 * time.Microsecond, 2 * time.Millisecond} {
			t.Run(fmt.Sprintf("%x/%s", version, delay), func(t *testing.T) {
				params := streamParameters()
				params.ActiveConnectionIDLimit = 4
				client, template := testPair(t, version, params)
				client.config.EnableMigration = true
				template.config.EnableMigration = true
				template.config.Parameters.DisableActiveMigration = false
				open := func() *net.UDPConn {
					t.Helper()
					s, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { s.Close() })
					return s
				}
				ss, cs := open(), open()
				codec, _ := token.NewRetryTokens([32]byte{18}, nil, time.Minute)
				endpoint, err := NewServerEndpoint(template.config, codec, 4)
				if err != nil {
					t.Fatal(err)
				}
				sr, err := NewUDPServer(ss, endpoint)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { sr.Close() })
				ce, err := NewClientEndpoint(client, ss.LocalAddr().(*net.UDPAddr).AddrPort())
				if err != nil {
					t.Fatal(err)
				}
				cr, err := NewUDPServer(cs, ce)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { cr.Close() })
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				server, _, err := sr.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err := cr.Wait(ctx, func(*ServerEndpoint) (bool, error) { return client.confirmed && client.multipath != nil, nil }); err != nil {
					t.Fatal(err)
				}
				old, _, _ := client.CurrentPathAddresses(0)
				var moved netip.AddrPort
				if delay < 0 {
					results := make(chan error, 8)
					start := make(chan struct{})
					for range 8 {
						go func() { <-start; results <- cr.Migrate(ctx, client, netip.AddrPortFrom(old.Addr(), 0)) }()
					}
					close(start)
					successes := 0
					for range 8 {
						result := <-results
						if result == nil {
							successes++
						} else if !errors.Is(result, errPathInitiation) {
							t.Errorf("concurrent migration: %v", result)
						}
					}
					if successes == 0 {
						t.Fatal("no concurrent migration committed")
					}
					moved, _, _ = client.CurrentPathAddresses(0)
					if moved == old {
						t.Fatal("concurrent migration retained initial binding")
					}
				} else {
					request, stop := context.WithCancel(ctx)
					canceled := make(chan struct{})
					go func() {
						defer close(canceled)
						timer := time.NewTimer(delay)
						defer timer.Stop()
						select {
						case <-timer.C:
							stop()
						case <-ctx.Done():
							stop()
						}
					}()
					err = cr.Migrate(request, client, netip.AddrPortFrom(old.Addr(), 0))
					<-canceled
					if err != nil && !errors.Is(err, context.Canceled) {
						t.Fatal("unexpected migration outcome", err)
					}
					moved, _, _ = client.CurrentPathAddresses(0)
					if (err == nil) == (moved == old) {
						t.Fatalf("result %v disagrees with binding %s -> %s", err, old, moved)
					}
				}
				if err := cr.Wait(ctx, func(e *ServerEndpoint) (bool, error) {
					app := client.multipath.paths[0].application
					if app.localMigration != nil || len(e.locals) != 1 || !e.locals[moved] {
						return true, errors.New("migration cleanup retained candidate or incorrect socket")
					}
					return cr.readers.Load() == 1, nil
				}); err != nil {
					t.Fatal(err)
				}
				// A cancellation can race peer validation too. Ordinary traffic must still
				// recover the selected local binding and finish a reliable stream.
				if err := cr.Do(ctx, func(*ServerEndpoint) error {
					id, err := client.OpenStream(false)
					if err != nil {
						return err
					}
					_, err = client.WriteStream(id, []byte("after race"), true)
					return err
				}); err != nil {
					t.Fatal(err)
				}
				var id uint64
				accepted := false
				var data []byte
				if err := sr.Wait(ctx, func(*ServerEndpoint) (bool, error) {
					if !accepted {
						var err error
						id, err = server.AcceptStream(false)
						if errors.Is(err, stream.ErrWouldBlock) {
							return false, nil
						}
						if err != nil {
							return true, err
						}
						accepted = true
					}
					var buf [32]byte
					n, err := server.ReadStream(id, buf[:])
					data = append(data, buf[:n]...)
					if errors.Is(err, stream.ErrWouldBlock) {
						return false, nil
					}
					if err == nil {
						return false, nil
					}
					if err != io.EOF {
						return true, err
					}
					if string(data) != "after race" {
						return true, errors.New("incorrect stream payload")
					}
					_, err = server.WriteStream(id, []byte("race reply"), true)
					return true, err
				}); err != nil {
					t.Fatal(err)
				}
				var reply []byte
				if err := cr.Wait(ctx, func(*ServerEndpoint) (bool, error) {
					var buf [32]byte
					n, err := client.ReadStream(id, buf[:])
					reply = append(reply, buf[:n]...)
					if errors.Is(err, stream.ErrWouldBlock) || err == nil {
						return false, nil
					}
					if err != io.EOF {
						return true, err
					}
					if string(reply) != "race reply" {
						return true, errors.New("incorrect race reply")
					}
					return true, nil
				}); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}
