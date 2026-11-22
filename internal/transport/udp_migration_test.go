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

func TestUDPActorLocalMigration(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, multipath := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/multipath=%v", version, multipath), func(t *testing.T) {
				var client, template *Conn
				if multipath {
					maximum := uint64(3)
					client, template = negotiatedPathPair(t, version, &maximum, &maximum)
				} else {
					client, template = testPair(t, version, streamParameters())
				}
				client.config.EnableMigration = true
				template.config.EnableMigration = true
				// testPair's already-built template advertised the legacy policy;
				// construct the runtime server with active migration permitted.
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
				ss, cs, nextSocket := open(), open(), open()
				codec, _ := token.NewRetryTokens([32]byte{17}, nil, time.Minute)
				endpoint, err := NewServerEndpoint(template.config, codec, 4)
				if err != nil {
					t.Fatal(err)
				}
				sr, err := NewUDPServer(ss, endpoint)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { sr.Close() })
				remote := ss.LocalAddr().(*net.UDPAddr).AddrPort()
				ce, err := NewClientEndpoint(client, remote)
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
				nextSocket.Close()
				if err := cr.Migrate(ctx, client, nextSocket.LocalAddr().(*net.UDPAddr).AddrPort()); err != nil {
					t.Fatal(err)
				}
				next, _, _ := client.CurrentPathAddresses(0)
				if err := cr.Wait(ctx, func(*ServerEndpoint) (bool, error) {
					local, _, ok := client.CurrentPathAddresses(0)
					return ok && local == next, nil
				}); err != nil {
					t.Fatal(err)
				}
				// The original socket can retire without abandoning the now migrated path.
				if err := cr.removeSocket(ctx, cs.LocalAddr().(*net.UDPAddr).AddrPort()); err != nil {
					t.Fatal(err)
				}
				if err := cr.Do(ctx, func(*ServerEndpoint) error {
					id, err := client.OpenStream(false)
					if err != nil {
						return err
					}
					_, err = client.WriteStream(id, []byte("migrated"), true)
					return err
				}); err != nil {
					t.Fatal(err)
				}
				var accepted uint64
				have := false
				if err := sr.Wait(ctx, func(*ServerEndpoint) (bool, error) {
					if !have {
						id, err := server.AcceptStream(false)
						if errors.Is(err, stream.ErrWouldBlock) {
							return false, nil
						}
						if err != nil {
							return false, err
						}
						accepted, have = id, true
					}
					var buf [32]byte
					n, err := server.ReadStream(accepted, buf[:])
					if errors.Is(err, stream.ErrWouldBlock) {
						return false, nil
					}
					if err != io.EOF || string(buf[:n]) != "migrated" {
						return false, errors.New("migration stream mismatch")
					}
					return true, nil
				}); err != nil {
					t.Fatal(err)
				}
				if err := sr.Do(ctx, func(*ServerEndpoint) error { server.config.allowCandidateMigration = false; return nil }); err != nil {
					t.Fatal(err)
				}
				limited, cancelMigration := context.WithTimeout(ctx, 100*time.Millisecond)
				err = cr.Migrate(limited, client, netip.AddrPortFrom(next.Addr(), 0))
				cancelMigration()
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("blocked migration did not cancel", err)
				}
				if err := cr.Do(ctx, func(e *ServerEndpoint) error {
					app := client.multipath.paths[0].application
					if app.localMigration != nil || app.network.local != next || len(e.locals) != 1 {
						return errors.New("canceled migration retained candidate socket or changed binding")
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}

			})
		}
	}
}
