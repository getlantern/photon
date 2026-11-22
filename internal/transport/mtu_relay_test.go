package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/wire"
)

// Real UDP sockets run through a userspace size filter. This exercises runtime
// timers and ACK/loss processing, but does not emulate a kernel route MTU.
func TestUDPDiscoveryConstrainedRelay(t *testing.T) { testUDPConstrainedRelay(t, false) }
func TestUDPDiscoveredMTUShrinks(t *testing.T)      { testUDPConstrainedRelay(t, true) }

func testUDPConstrainedRelay(t *testing.T, shrink bool) {
	if !udpSupportsMTUDiscovery {
		t.Skip("nonfragmenting sockets unsupported")
	}
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprintf("%x", v), func(t *testing.T) {
			t.Parallel()
			client, template := testPair(t, v, streamParameters())
			client.config.EnableMigration = true
			template.config.EnableMigration = true
			open := func() *net.UDPConn {
				s, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { s.Close() })
				return s
			}
			ss, cs, relay := open(), open(), open()
			sa, ca := ss.LocalAddr().(*net.UDPAddr).AddrPort(), cs.LocalAddr().(*net.UDPAddr).AddrPort()
			const limit = 1388
			var currentLimit atomic.Int64
			currentLimit.Store(limit)
			var dropped atomic.Int64
			relayDone := make(chan error, 1)
			go func() {
				var buf [65535]byte
				for {
					n, from, err := relay.ReadFromUDPAddrPort(buf[:])
					if err != nil {
						relayDone <- err
						return
					}
					if int64(n) > currentLimit.Load() {
						dropped.Add(1)
						continue
					}
					to := sa
					if from == sa {
						to = ca
					} else if from != ca {
						continue
					}
					if _, err := relay.WriteToUDPAddrPort(buf[:n], to); err != nil {
						relayDone <- err
						return
					}
				}
			}()
			defer func() {
				relay.Close()
				if err := <-relayDone; err != nil && !errors.Is(err, net.ErrClosed) {
					t.Error(err)
				}
			}()
			codec, _ := token.NewRetryTokens([32]byte{31}, nil, time.Minute)
			ep, err := NewServerEndpoint(template.config, codec, 4)
			if err != nil {
				t.Fatal(err)
			}
			sr, err := NewUDPServer(ss, ep)
			if err != nil {
				t.Fatal(err)
			}
			defer sr.Close()
			ce, err := NewClientEndpoint(client, relay.LocalAddr().(*net.UDPAddr).AddrPort())
			if err != nil {
				t.Fatal(err)
			}
			cr, err := NewUDPServer(cs, ce)
			if err != nil {
				t.Fatal(err)
			}
			defer cr.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
			defer cancel()
			server, _, err := sr.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			// Ordinary small traffic supplies later ACKs for packet/time-threshold loss
			// detection; no recovery or discovery state is modified by this driver.
			ticker := time.NewTicker(25 * time.Millisecond)
			defer ticker.Stop()
			for {
				settled := true
				for _, p := range []struct {
					r *UDPServer
					c *Conn
				}{{cr, client}, {sr, server}} {
					if err := p.r.Do(ctx, func(*ServerEndpoint) error {
						if !p.c.confirmed || p.c.multipath == nil {
							settled = false
							return nil
						}
						a := p.c.multipath.paths[0].application
						if a.mtu == nil || !a.completedLowerMTU() {
							settled = false
							return p.c.queueFrameOnPath(0, wire.PingFrame{})
						}
						if a.mtu.confirmed != limit {
							return fmt.Errorf("settled MTU %d, want %d", a.mtu.confirmed, limit)
						}
						return nil
					}); err != nil {
						t.Fatal(err)
					}
				}
				if settled {
					break
				}
				select {
				case <-ticker.C:
				case <-ctx.Done():
					t.Fatal("discovery convergence", ctx.Err())
				}
			}
			if dropped.Load() == 0 {
				t.Fatal("size filter never exercised")
			}
			dropsBefore := dropped.Load()
			if shrink {
				currentLimit.Store(1200)
			}
			payload := bytes.Repeat([]byte("relay"), 1000)
			if shrink {
				// Exactly three full-size writes: there is no small trailing STREAM
				// packet to provide reachability evidence after the size reduction.
				payload = payload[:3*(limit-(minimumQUICMTU-portableStreamPayload))]
			}
			var id uint64
			if err := cr.Do(ctx, func(*ServerEndpoint) error {
				var err error
				id, err = client.OpenStream(false)
				if err != nil {
					return err
				}
				for offset := 0; offset < len(payload); {
					n, err := client.WriteStream(id, payload[offset:], true)
					if err != nil {
						return err
					}
					if n == 0 {
						return fmt.Errorf("no write progress")
					}
					offset += n
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			var received []byte
			haveStream := false
			if err := sr.Wait(ctx, func(*ServerEndpoint) (bool, error) {
				if !haveStream {
					accepted, err := server.AcceptStream(false)
					if errors.Is(err, stream.ErrWouldBlock) {
						return false, nil
					}
					if err != nil {
						return false, err
					}
					if accepted != id {
						return false, fmt.Errorf("unexpected stream %d", accepted)
					}
					haveStream = true
				}
				var buf [6000]byte
				n, err := server.ReadStream(id, buf[:])
				received = append(received, buf[:n]...)
				if errors.Is(err, stream.ErrWouldBlock) {
					return false, nil
				}
				if errors.Is(err, io.EOF) {
					return true, nil
				}
				return false, err
			}); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(received, payload) {
				t.Fatal("payload mismatch", len(received))
			}
			if err := cr.Wait(ctx, func(*ServerEndpoint) (bool, error) { return client.StreamAcknowledged(id) == nil, nil }); err != nil {
				t.Fatal(err)
			}
			if shrink {
				if dropped.Load()-dropsBefore < 3 {
					t.Fatal("large data losses not exercised")
				}
				if err := cr.Do(ctx, func(*ServerEndpoint) error {
					a := client.multipath.paths[0].application
					if a.mtu.confirmed != 1200 || a.mtuNeedsValidation || a.validation.state != PathValidated || a.mtuRaiseDeadline().IsZero() {
						return fmt.Errorf("missing validated MTU fallback")
					}
					if client.queuedBytes != 0 {
						return fmt.Errorf("retained stream charge %d", client.queuedBytes)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}

		})
	}
}
