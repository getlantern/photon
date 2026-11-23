package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/wire"
)

// A front socket stays fixed from the client's perspective. Two back sockets
// model a NAT replacing its public source port without changing the client.
func TestUDPActorPortRebinding(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			maximum := uint64(3)
			client, template := negotiatedPathPair(t, version, &maximum, &maximum)
			cfg := template.config
			cfg.allowCandidateMigration = true
			tokens, _ := token.NewRetryTokens([32]byte{19}, nil, time.Minute)
			endpoint, err := NewServerEndpoint(cfg, tokens, 4)
			if err != nil {
				t.Fatal(err)
			}
			open := func() *net.UDPConn {
				t.Helper()
				s, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { s.Close() })
				return s
			}
			ss, cs, front, oldBack, newBack := open(), open(), open(), open(), open()
			var active atomic.Pointer[net.UDPConn]
			active.Store(oldBack)
			var replies atomic.Int64
			var relay sync.WaitGroup
			relay.Add(3)
			go func() {
				defer relay.Done()
				buffer := make([]byte, 65535)
				for {
					n, _, err := front.ReadFromUDP(buffer)
					if err != nil {
						return
					}
					if _, err := active.Load().WriteToUDP(buffer[:n], ss.LocalAddr().(*net.UDPAddr)); err != nil {
						return
					}
				}
			}()
			for _, back := range []*net.UDPConn{oldBack, newBack} {
				go func() {
					defer relay.Done()
					buffer := make([]byte, 65535)
					for {
						n, _, err := back.ReadFromUDP(buffer)
						if err != nil {
							return
						}
						if back == newBack {
							replies.Add(1)
						}
						if _, err := front.WriteToUDP(buffer[:n], cs.LocalAddr().(*net.UDPAddr)); err != nil {
							return
						}
					}
				}()
			}
			defer func() { front.Close(); oldBack.Close(); newBack.Close(); relay.Wait() }()
			sr, err := NewUDPServer(ss, endpoint)
			if err != nil {
				t.Fatal(err)
			}
			defer sr.Close()
			ce, err := NewClientEndpoint(client, front.LocalAddr().(*net.UDPAddr).AddrPort())
			if err != nil {
				t.Fatal(err)
			}
			cr, err := NewUDPServer(cs, ce)
			if err != nil {
				t.Fatal(err)
			}
			defer cr.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			server, _, err := sr.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := cr.Wait(ctx, func(*ServerEndpoint) (bool, error) { return client.multipath != nil, client.Err() }); err != nil {
				t.Fatal(err)
			}
			transfer := func(sender *UDPServer, from *Conn, receiver *UDPServer, to *Conn, message string) {
				t.Helper()
				if err := sender.Do(ctx, func(*ServerEndpoint) error {
					id, err := from.OpenStream(false)
					if err != nil {
						return err
					}
					n, err := from.WriteStream(id, []byte(message), true)
					if err == nil && n != len(message) {
						return io.ErrShortWrite
					}
					return err
				}); err != nil {
					t.Fatal(err)
				}
				var id uint64
				have := false
				var received []byte
				if err := receiver.Wait(ctx, func(*ServerEndpoint) (bool, error) {
					if err := to.Err(); err != nil {
						return true, err
					}
					if !have {
						opened, err := to.AcceptStream(false)
						if errors.Is(err, stream.ErrWouldBlock) {
							return false, nil
						}
						if err != nil {
							return true, err
						}
						id, have = opened, true
					}
					var buffer [128]byte
					n, err := to.ReadStream(id, buffer[:])
					received = append(received, buffer[:n]...)
					if err == nil || errors.Is(err, stream.ErrWouldBlock) {
						return false, nil
					}
					if err != io.EOF {
						return true, err
					}
					if string(received) != message {
						return true, fmt.Errorf("received %q", received)
					}
					return true, nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			transfer(cr, client, sr, server, "before rebinding")
			active.Store(newBack)
			transfer(cr, client, sr, server, "after rebinding")
			if err := sr.Do(ctx, func(*ServerEndpoint) error {
				app := server.multipath.paths[0].application
				if app.network.remote != newBack.LocalAddr().(*net.UDPAddr).AddrPort() || app.migration == nil || app.migration.candidate != nil || !app.budget.Validated() {
					return errors.New("server did not commit validated relay port")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if server.CurrentRemoteAddr() != newBack.LocalAddr().(*net.UDPAddr).AddrPort() {
				t.Fatal("published peer address did not follow rebinding")
			}
			before := replies.Load()
			transfer(sr, server, cr, client, "return on rebound path")
			if replies.Load() <= before {
				t.Fatal("no return traffic on rebound socket")
			}
			active.Store(oldBack)
			transfer(cr, client, sr, server, "second rebinding")
			if err := sr.Do(ctx, func(*ServerEndpoint) error {
				app := server.multipath.paths[0].application
				if app.network.remote != oldBack.LocalAddr().(*net.UDPAddr).AddrPort() || app.migration.candidate != nil {
					return errors.New("return to original relay port did not commit")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			transfer(sr, server, cr, client, "return after second rebinding")
			if server.CurrentRemoteAddr() != oldBack.LocalAddr().(*net.UDPAddr).AddrPort() {
				t.Fatal("published peer address did not follow second rebinding")
			}
		})
	}
}
