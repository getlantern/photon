package interop

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/wire"
	upstream "github.com/quic-go/quic-go"
)

func TestPublicEarlyPartialResetReceiverUpstream(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, uni := range []bool{false, true} {
			for _, mode := range []string{"accept", "loss", "reject", "withdrawn"} {
				t.Run(fmt.Sprintf("%x/uni=%v/%s", v, uni, mode), func(t *testing.T) {
					ct, st := tlsConfigs(t)
					cache := &observedSessionCache{ClientSessionCache: tls.NewLRUClientSessionCache(4), stored: make(chan bool, 8)}
					ct.ClientSessionCache = cache
					var resumed atomic.Bool
					l, err := quic.ListenAddrEarly("127.0.0.1:0", st, &quic.Config{Versions: []quic.Version{v}, Allow0RTT: true, GetConfigForClient: func(*quic.ClientInfo) (*quic.Config, error) {
						return &quic.Config{Allow0RTT: !resumed.Load() || mode != "reject", EnableStreamResetPartialDelivery: !resumed.Load() || mode != "withdrawn"}, nil
					}})
					if err != nil {
						t.Fatal(err)
					}
					defer l.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					cfg := &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}}
					seed, err := dialUpstreamAddr(t, ctx, l.Addr().String(), ct, cfg)
					if err != nil {
						t.Fatal(err)
					}
					defer seed.CloseWithError(0, "")
					if _, err := l.Accept(ctx); err != nil {
						t.Fatal(err)
					}
					select {
					case enabled := <-cache.stored:
						if !enabled {
							t.Fatal("no early ticket")
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					resumed.Store(true)
					udp, err := net.ListenPacket("udp4", "127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					dropper := &dropFirstEarlyWritePacket{PacketConn: udp, enabled: mode == "loss"}
					gate := &earlyPacketGate{PacketConn: dropper, seen: make(chan struct{}), done: make(chan struct{})}
					release := make(chan struct{})
					var once sync.Once
					unblock := func() { once.Do(func() { close(release) }) }
					defer unblock()
					tr := &upstream.Transport{Conn: &heldServerFlight{earlyPacketGate: gate, release: release}}
					defer func() { gate.Close(); tr.Close() }()
					stop := context.AfterFunc(ctx, func() { gate.Close() })
					defer stop()
					client, err := tr.DialEarly(ctx, l.Addr(), ct, cfg)
					if err != nil {
						t.Fatal(err)
					}
					defer client.CloseWithError(0, "")
					type sender interface {
						Write([]byte) (int, error)
						Close() error
						SetReliableBoundary()
						CancelWrite(upstream.StreamErrorCode)
						StreamID() upstream.StreamID
					}
					open := func(c *upstream.Conn) (sender, error) {
						if uni {
							return c.OpenUniStreamSync(ctx)
						}
						return c.OpenStreamSync(ctx)
					}
					tx, err := open(client)
					if err != nil {
						t.Fatal(err)
					}
					prefix := strings.Repeat("early reliable prefix", 100)
					if n, err := tx.Write([]byte(prefix)); err != nil || n != len(prefix) {
						t.Fatal("write", n, err)
					}
					tx.SetReliableBoundary()
					tx.CancelWrite(7)
					select {
					case <-gate.seen:
					case <-ctx.Done():
						t.Fatal("no early packet", ctx.Err())
					}
					accepted := mode == "accept" || mode == "loss"
					if !accepted {
						unblock()
					}
					peer, err := l.Accept(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if !accepted {
						next, err := client.NextConnection(ctx)
						if err != nil {
							t.Fatal("resume after rejection", err)
						}
						fresh, err := open(next)
						if err != nil || fresh.StreamID() != tx.StreamID() {
							t.Fatal("fresh stream", err)
						}
						tx.SetReliableBoundary()
						tx.CancelWrite(99)
						prefix = "fresh after rejected partial reset"
						if _, err := fresh.Write([]byte(prefix)); err != nil {
							t.Fatal(err)
						}
						if err := fresh.Close(); err != nil {
							t.Fatal(err)
						}
					} else {
						select {
						case <-peer.HandshakeComplete():
							t.Fatal("delivery did not happen early")
						default:
						}
					}
					var rx interface {
						Read([]byte) (int, error)
						SetReadDeadline(time.Time) error
						Release(context.Context) error
					}
					var bidi *quic.Stream
					if uni {
						rx, err = peer.AcceptUniStream(ctx)
					} else {
						bidi, err = peer.AcceptStream(ctx)
						rx = bidi
					}
					if err != nil {
						t.Fatal(err)
					}
					// Application-data PTO recovery at the reference waits for
					// handshake confirmation. Keep the strict early-read barrier
					// only for the no-loss case.
					if mode == "loss" {
						unblock()
					}
					deadline, _ := ctx.Deadline()
					rx.SetReadDeadline(deadline)
					got, err := io.ReadAll(rx)
					if string(got) != prefix {
						t.Fatal("prefix", len(got), err)
					}
					if accepted {
						var reset *quic.StreamError
						if !errors.As(err, &reset) || reset.ErrorCode != 7 || !reset.Remote {
							t.Fatal("early reset", err)
						}
						if mode == "accept" {
							select {
							case <-peer.HandshakeComplete():
								t.Fatal("prefix/reset delivered after Finished")
							default:
							}
						}
					} else if err != nil {
						t.Fatal("rejected reset affected fresh stream", err)
					}
					unblock()
					if bidi != nil {
						bidi.CancelWrite(0)
					}
					if err := rx.Release(ctx); err != nil {
						t.Fatal("release", err)
					}
					for _, done := range []<-chan struct{}{peer.HandshakeComplete(), client.HandshakeComplete()} {
						select {
						case <-done:
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
					}
					if mode == "loss" && !dropper.dropped.Load() {
						t.Fatal("loss fixture did not drop post-Retry early packet")
					}
					state := peer.ConnectionState()
					if !state.TLS.DidResume || !client.ConnectionState().TLS.DidResume || state.Used0RTT != accepted || client.ConnectionState().Used0RTT != accepted {
						t.Fatal("resumption result", state.Used0RTT, state.TLS.DidResume)
					}
					flags := state.SupportsStreamResetPartialDelivery
					if flags.Local != (mode != "withdrawn") || flags.Remote || client.ConnectionState().SupportsStreamResetPartialDelivery.Remote != flags.Local {
						t.Fatal("selected capability", flags)
					}
				})
			}
		}
	}
}

// Strip the first 0-RTT packet addressed to the Retry CID, preserving any
// coalesced Initial. Pre-Retry early traffic would be discarded anyway. Returning
// the original datagram length models network loss after a successful UDP send.
type dropFirstEarlyWritePacket struct {
	net.PacketConn
	enabled  bool
	dropped  atomic.Bool
	retryCID atomic.Pointer[[]byte]
}

func (d *dropFirstEarlyWritePacket) WriteTo(b []byte, addr net.Addr) (int, error) {
	retryCID := d.retryCID.Load()
	if !d.enabled || d.dropped.Load() || retryCID == nil {
		return d.PacketConn.WriteTo(b, addr)
	}
	for offset := 0; offset < len(b); {
		h, err := wire.ParseHeader(b[offset:], 0)
		if err != nil || h.PacketLen <= 0 {
			break
		}
		if h.Type == wire.ZeroRTT && bytes.Equal(h.DCID, *retryCID) && d.dropped.CompareAndSwap(false, true) {
			rest := append(append([]byte(nil), b[:offset]...), b[offset+h.PacketLen:]...)
			if len(rest) > 0 {
				n, err := d.PacketConn.WriteTo(rest, addr)
				if err != nil {
					return 0, err
				}
				if n != len(rest) {
					return 0, io.ErrShortWrite
				}
			}
			return len(b), nil
		}
		offset += h.PacketLen
	}
	return d.PacketConn.WriteTo(b, addr)
}

func (d *dropFirstEarlyWritePacket) ReadFrom(b []byte) (int, net.Addr, error) {
	n, addr, err := d.PacketConn.ReadFrom(b)
	if err == nil {
		h, parseErr := wire.ParseHeader(b[:n], 0)
		if parseErr == nil && h.Type == wire.Retry {
			// Duplicate Initials can elicit multiple Retry responses; the
			// client accepts the first and ignores later replies.
			cid := bytes.Clone(h.SCID)
			d.retryCID.CompareAndSwap(nil, &cid)
		}
	}
	return n, addr, err
}
