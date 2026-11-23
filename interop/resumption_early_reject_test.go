package interop

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/wire"
	upstream "github.com/quic-go/quic-go"
)

// Hold server input until a real 0-RTT packet has left the client's socket.
// This removes the race between an early stream write and handshake completion.
type earlyPacketGate struct {
	net.PacketConn
	seen, done      chan struct{}
	once, closeOnce sync.Once
}

func (g *earlyPacketGate) ReadFrom(b []byte) (int, net.Addr, error) {
	select {
	case <-g.seen:
	case <-g.done:
		return 0, nil, net.ErrClosed
	}
	return g.PacketConn.ReadFrom(b)
}
func (g *earlyPacketGate) WriteTo(b []byte, a net.Addr) (int, error) {
	early := false
	for rest := b; len(rest) > 0; {
		h, err := wire.ParseHeader(rest, 0)
		if err != nil || h.PacketLen <= 0 {
			break
		}
		if h.Type == wire.ZeroRTT {
			early = true
		}
		rest = rest[h.PacketLen:]
	}
	n, err := g.PacketConn.WriteTo(b, a)
	if err == nil && n == len(b) && early {
		g.once.Do(func() { close(g.seen) })
	}
	return n, err
}
func (g *earlyPacketGate) Close() error {
	g.closeOnce.Do(func() { close(g.done) })
	return g.PacketConn.Close()
}

func TestServerRejectsExternalEarlyTicket(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", v), func(t *testing.T) {
			clientTLS, serverTLS := tlsConfigs(t)
			cache := &sharedTicketCache{stored: make(chan struct{}, 1)}
			clientTLS.ClientSessionCache = cache
			var key [32]byte
			key[0] = 73 // Test-only cross-implementation ticket key.
			serverTLS.SetSessionTicketKeys([][32]byte{key})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			peerCfg := &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}, Allow0RTT: true}
			seedServer, err := upstream.ListenAddr("127.0.0.1:0", serverTLS, peerCfg)
			if err != nil {
				t.Fatal(err)
			}
			defer seedServer.Close()
			seed, err := dialUpstreamAddr(t, ctx, seedServer.Addr().String(), clientTLS, peerCfg)
			if err != nil {
				t.Fatal(err)
			}
			defer seed.CloseWithError(0, "")
			seedPeer, err := seedServer.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer seedPeer.CloseWithError(0, "")
			select {
			case <-cache.stored:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			_, state, err := cache.freeze().ResumptionState()
			if err != nil || !state.EarlyData {
				t.Fatal("seed did not enable early data", err)
			}
			seed.CloseWithError(0, "")
			seedPeer.CloseWithError(0, "")
			seedServer.Close()

			listener, err := quic.ListenAddr("127.0.0.1:0", serverTLS, &quic.Config{Versions: []quic.Version{v}})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			udp, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			gate := &earlyPacketGate{PacketConn: udp, seen: make(chan struct{}), done: make(chan struct{})}
			defer gate.Close()
			tr := &upstream.Transport{Conn: gate}
			defer func() { gate.Close(); tr.Close() }()
			// Ensure an unsuccessful early dial can unblock its read goroutine.
			stop := context.AfterFunc(ctx, func() { gate.Close() })
			defer stop()
			c, err := tr.DialEarly(ctx, listener.Addr(), clientTLS, peerCfg)
			if err != nil {
				t.Fatal(err)
			}
			defer c.CloseWithError(0, "")
			early, err := c.OpenUniStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			early.SetWriteDeadline(time.Now().Add(time.Second))
			if _, err := early.Write([]byte("must not arrive")); err != nil && !errors.Is(err, upstream.Err0RTTRejected) {
				t.Fatal(err)
			}
			if err := early.Close(); err != nil && !errors.Is(err, upstream.Err0RTTRejected) {
				t.Fatal(err)
			}
			select {
			case <-gate.seen:
			case <-ctx.Done():
				t.Fatal("no 0-RTT packet sent", ctx.Err())
			}
			peer, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-c.HandshakeComplete():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if c.ConnectionState().TLS.DidResume || peer.ConnectionState().TLS.DidResume || c.ConnectionState().Used0RTT {
				t.Fatal("expected full handshake after unversioned ticket rejection")
			}
			if _, err := c.OpenUniStream(); !errors.Is(err, upstream.Err0RTTRejected) {
				t.Fatal("missing early rejection", err)
			}
			c, err = c.NextConnection(ctx)
			if err != nil {
				t.Fatal(err)
			}
			tx, err := c.OpenUniStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			tx.SetWriteDeadline(time.Now().Add(time.Second))
			if _, err := tx.Write([]byte("one RTT only")); err != nil {
				t.Fatal(err)
			}
			if err := tx.Close(); err != nil {
				t.Fatal(err)
			}
			rx, err := peer.AcceptUniStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			rx.SetReadDeadline(time.Now().Add(time.Second))
			if b, err := io.ReadAll(rx); err != nil || string(b) != "one RTT only" {
				t.Fatalf("early bytes leaked or recovery failed: %q %v", b, err)
			}
		})
	}
}
