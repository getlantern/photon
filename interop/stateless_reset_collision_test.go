package interop

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

// Rewriting the reset's unpredictable prefix preserves its valid upstream token
// while forcing a collision with a different connection's local CID.
type collisionResetSocket struct {
	resetRestartSocket
	longVersion uint32 // immutable; nonzero crafts a long-header reset
	targetCID   atomic.Pointer[[8]byte]
	rewrite     atomic.Bool
	rewritten   atomic.Int64
}

func (s *collisionResetSocket) WriteTo(p []byte, addr net.Addr) (int, error) {
	originalLength := len(p)
	if s.rewrite.Load() && len(p) >= 25 && p[0]&0x80 == 0 {
		p = append([]byte(nil), p...)
		if cid := s.targetCID.Load(); cid != nil {
			if s.longVersion == 0 {
				copy(p[1:9], cid[:])
			} else {
				// A framing-valid Handshake prefix, one PN byte and the original
				// upstream reset token. RFC 9000 requires accepting this despite
				// requiring senders to generate short-header resets.
				long := make([]byte, 33)
				long[0] = 0xe0
				if s.longVersion == uint32(quic.Version2) {
					long[0] = 0xf0
				}
				binary.BigEndian.PutUint32(long[1:5], s.longVersion)
				long[5] = 8
				copy(long[6:14], cid[:])
				long[15] = 17 // PN plus token length; SCID length is zero.
				copy(long[17:], p[len(p)-16:])
				p = long
			}
		} else {
			return 0, fmt.Errorf("collision fixture has no live CID")
		}
		s.rewritten.Add(1)
	}
	n, err := s.resetRestartSocket.WriteTo(p, addr)
	if err == nil && n == len(p) {
		return originalLength, nil
	}
	return n, err
}

// Capture a CID actually being used by the stable peer. Initial CIDs may be
// retired during the handshake, so assuming a generator sequence is insufficient.
type collisionCIDObserver struct {
	net.PacketConn
	stable string
	target *atomic.Pointer[[8]byte]
}

func (s *collisionCIDObserver) ReadFrom(p []byte) (int, net.Addr, error) {
	n, peer, err := s.PacketConn.ReadFrom(p)
	if err == nil && peer.String() == s.stable && n >= 9 && p[0]&0x80 == 0 {
		cid := new([8]byte)
		copy(cid[:], p[1:9])
		s.target.Store(cid)
	}
	return n, peer, err
}

func TestStatelessResetCIDCollisionIndependent(t *testing.T) {
	testStatelessResetCIDCollision(t, false, false, false)
}
func TestStatelessResetForeignCIDCollisionIndependent(t *testing.T) {
	testStatelessResetCIDCollision(t, false, true, false)
}

func TestStatelessResetForeignFirstLocalTargetIndependent(t *testing.T) {
	testStatelessResetCIDCollision(t, true, false, false)
}
func TestStatelessResetForeignFirstForeignTargetIndependent(t *testing.T) {
	testStatelessResetCIDCollision(t, true, true, false)
}

func TestStatelessResetLongHeaderCollisionIndependent(t *testing.T) {
	for _, first := range []bool{false, true} {
		for _, target := range []bool{false, true} {
			t.Run(fmt.Sprintf("first-foreign=%v/target-foreign=%v", first, target), func(t *testing.T) {
				testStatelessResetCIDCollision(t, first, target, true)
			})
		}
	}
}

func testStatelessResetCIDCollision(t *testing.T, foreignFirst, foreign, longHeader bool) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ct, st := tlsConfigs(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cfg := &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}
			stable, err := upstream.ListenAddr("127.0.0.1:0", st, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer stable.Close()
			socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer socket.Close()
			wrapped := &collisionResetSocket{resetRestartSocket: resetRestartSocket{PacketConn: socket}}
			if longHeader {
				wrapped.longVersion = uint32(version)
			}
			key := upstream.StatelessResetKey{0x42}
			restarting := &upstream.Transport{Conn: wrapped, StatelessResetKey: &key}
			defer restarting.Close()
			listener, err := restarting.Listen(st, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			clientSocket, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer clientSocket.Close()
			observed := &collisionCIDObserver{PacketConn: clientSocket, stable: stable.Addr().String(), target: &wrapped.targetCID}
			shared := &quic.Transport{Conn: observed, ConnectionIDLength: 8}
			defer shared.Close()
			clientCfg := &quic.Config{Versions: []quic.Version{version}}
			makeTransport := func(foreign bool) (*quic.Transport, *resetProbePrimarySocket) {
				if !foreign {
					return shared, nil
				}
				originalSocket, err := net.ListenPacket("udp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				original := &resetProbePrimarySocket{resetRestartSocket: resetRestartSocket{PacketConn: originalSocket}}
				transport := &quic.Transport{Conn: original, ConnectionIDLength: 8}
				t.Cleanup(func() { transport.Close(); originalSocket.Close() })
				return transport, original
			}
			migrate := func(c *quic.Conn, original *resetProbePrimarySocket) {
				if original == nil {
					return
				}
				path, err := c.AddPath(shared)
				if err != nil {
					t.Fatal(err)
				}
				if err := path.Probe(ctx); err != nil {
					t.Fatal(err)
				}
				if err := path.Switch(); err != nil {
					t.Fatal(err)
				}
				original.muted.Store(true)
			}
			aTransport, aOriginal := makeTransport(foreignFirst)
			a, err := aTransport.Dial(ctx, stable.Addr(), ct, clientCfg)
			if err != nil {
				t.Fatal(err)
			}
			defer a.CloseWithError(0, "")
			peerA, err := stable.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			migrate(a, aOriginal)
			bTransport, original := makeTransport(foreign)
			b, err := bTransport.Dial(ctx, listener.Addr(), ct, clientCfg)
			if err != nil {
				t.Fatal(err)
			}
			defer b.CloseWithError(0, "")
			peerB, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			migrate(b, original)
			transfer := func(client *quic.Conn, peer *upstream.Conn) *quic.Stream {
				t.Helper()
				tx, err := client.OpenStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				payload := make([]byte, 128)
				if _, err := tx.Write(payload); err != nil {
					t.Fatal(err)
				}
				rx, err := peer.AcceptStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				rx.SetReadDeadline(time.Now().Add(time.Second))
				if _, err := io.ReadFull(rx, payload); err != nil {
					t.Fatal(err)
				}
				if _, err := rx.Write(payload); err != nil {
					t.Fatal(err)
				}
				tx.SetReadDeadline(time.Now().Add(time.Second))
				if _, err := io.ReadFull(tx, payload); err != nil {
					t.Fatal(err)
				}
				return tx
			}
			transfer(a, peerA)
			if wrapped.targetCID.Load() == nil {
				t.Fatal("stable peer supplied no CID")
			}
			trigger := transfer(b, peerB)
			wrapped.muted.Store(true)
			if err := restarting.Close(); err != nil {
				t.Fatal(err)
			}
			fresh := &upstream.Transport{Conn: wrapped, StatelessResetKey: &key}
			defer fresh.Close()
			freshListener, err := fresh.Listen(st, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer freshListener.Close()
			wrapped.rewrite.Store(true)
			wrapped.muted.Store(false)
			if _, err := trigger.Write(make([]byte, 128)); err != nil {
				t.Fatal(err)
			}
			select {
			case <-b.Context().Done():
			case <-ctx.Done():
				t.Fatal("CID collision hid reset", ctx.Err())
			}
			var reset *quic.StatelessResetError
			if !errors.As(context.Cause(b.Context()), &reset) || wrapped.rewritten.Load() == 0 {
				t.Fatal("reset contract", context.Cause(b.Context()))
			}
			if a.Context().Err() != nil {
				t.Fatal("unrelated connection closed", context.Cause(a.Context()))
			}
			transfer(a, peerA)
		})
	}
}
