package interop

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/wire"
	upstream "github.com/quic-go/quic-go"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type earlyArrivalObserver struct {
	net.PacketConn
	seen chan struct{}
	once sync.Once

	// What arrived, reported by summary when a test finds no early packet.
	mu                  sync.Mutex
	datagrams, unparsed int
	types               map[wire.PacketType]int
	firstUnparsed       string
}

func (o *earlyArrivalObserver) ReadFrom(b []byte) (int, net.Addr, error) {
	n, a, err := o.PacketConn.ReadFrom(b)
	if err == nil {
		o.mu.Lock()
		o.datagrams++
		o.mu.Unlock()
		for rest := b[:n]; len(rest) > 0; {
			h, e := wire.ParseHeader(rest, 0)
			if e != nil || h.PacketLen <= 0 {
				o.mu.Lock()
				o.unparsed++
				if o.firstUnparsed == "" {
					o.firstUnparsed = fmt.Sprintf("%d bytes, first byte %#x: %v", len(rest), rest[0], e)
				}
				o.mu.Unlock()
				break
			}
			o.mu.Lock()
			if o.types == nil {
				o.types = make(map[wire.PacketType]int)
			}
			o.types[h.Type]++
			o.mu.Unlock()
			if h.Type == wire.ZeroRTT {
				o.once.Do(func() { close(o.seen) })
			}
			rest = rest[h.PacketLen:]
		}
	}
	return n, a, err
}

// summary describes every datagram the observer has read.
func (o *earlyArrivalObserver) summary() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return fmt.Sprintf("arrivals: %d datagrams, packet types %v, %d unparsed (first: %s)", o.datagrams, o.types, o.unparsed, o.firstUnparsed)
}

func TestEarlyClientAcceptanceUpstream(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, accept := range []bool{true, false} {
			t.Run(fmt.Sprintf("%x/accept=%v", v, accept), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				cache := &observedSessionCache{ClientSessionCache: tls.NewLRUClientSessionCache(4), stored: make(chan bool, 8)}
				ct.ClientSessionCache = cache
				gate := make(chan struct{})
				var once sync.Once
				release := func() { once.Do(func() { close(gate) }) }
				defer release()
				var hellos atomic.Int32
				st.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
					if hellos.Add(1) > 1 {
						<-gate
					}
					return nil, nil
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				for round := 0; round < 2; round++ {
					socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					arrival := &earlyArrivalObserver{PacketConn: socket, seen: make(chan struct{})}
					tr := &upstream.Transport{Conn: arrival}
					t.Cleanup(func() { socket.Close(); tr.Close() })
					l, err := tr.ListenEarly(st, &upstream.Config{Allow0RTT: round == 0 || accept, Versions: []upstream.Version{upstream.Version(v)}})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { l.Close() })

					c, err := quic.DialAddrEarly(ctx, l.Addr().String(), ct, &quic.Config{Versions: []quic.Version{v}})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { c.CloseWithError(0, "") })
					if round == 1 {
						select {
						case <-c.HandshakeComplete():
							t.Fatal("not early")
						default:
						}
					}
					tx, err := c.OpenUniStreamSync(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := tx.Write([]byte("independent early client")); err != nil {
						t.Fatal(err)
					}
					if err := tx.Close(); err != nil {
						t.Fatal(err)
					}
					if round == 1 {
						select {
						case <-arrival.seen:
						case <-ctx.Done():
							t.Fatal("no early client wire packet", ctx.Err())
						}
						release()
					}
					expected := "independent early client"
					if round == 1 && !accept {
						expected = "explicit recovered request"
						select {
						case <-c.HandshakeComplete():
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
						if _, err := c.OpenUniStream(); !errors.Is(err, quic.Err0RTTRejected) {
							t.Fatal("missing rejection gate", err)
						}
						recovered, err := c.NextConnection(ctx)
						if err != nil || recovered != c {
							t.Fatal("recovery", err)
						}
						if err := tx.WaitAcknowledged(ctx); !errors.Is(err, quic.Err0RTTRejected) {
							t.Fatal("stale FIN handle", err)
						}
						fresh, err := c.OpenUniStreamSync(ctx)
						if err != nil {
							t.Fatal(err)
						}
						if fresh.StreamID() != tx.StreamID() {
							t.Fatal("stream ID not reset")
						}
						tx.CancelWrite(42)
						if _, err := fresh.Write([]byte(expected)); err != nil {
							t.Fatal(err)
						}
						if err := fresh.Close(); err != nil {
							t.Fatal(err)
						}
					}
					peer, err := l.Accept(ctx)
					if err != nil {
						t.Fatal(err)
					}
					rx, err := peer.AcceptUniStream(ctx)
					if err != nil {
						t.Fatal(err)
					}
					rx.SetReadDeadline(time.Now().Add(time.Second))
					b, err := io.ReadAll(rx)
					if err != nil || string(b) != expected {
						t.Fatal(string(b), err)
					}
					select {
					case <-c.HandshakeComplete():
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					if c.ConnectionState().Used0RTT != (round == 1 && accept) || peer.ConnectionState().Used0RTT != (round == 1 && accept) {
						t.Fatal("early acceptance mismatch")
					}
					if round == 0 {
						select {
						case enabled := <-cache.stored:
							if !enabled {
								t.Fatal("ticket disabled early data")
							}
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
					}
				}
			})
		}
	}
}

// Permit Retry, then hold the TLS flight until the server application consumes
// early data. The client cannot send Finished before the assertions run.
type heldServerFlight struct {
	retries atomic.Int32
	*earlyPacketGate
	release <-chan struct{}
}

func (g *heldServerFlight) ReadFrom(b []byte) (int, net.Addr, error) {
	n, a, err := g.earlyPacketGate.ReadFrom(b)
	if err != nil {
		return n, a, err
	}
	h, parseErr := wire.ParseHeader(b[:n], 0)
	if parseErr == nil && h.Type == wire.Retry {
		g.retries.Add(1)
		return n, a, nil
	}
	select {
	case <-g.release:
		return n, a, nil
	case <-g.done:
		return 0, nil, net.ErrClosed
	}
}

func TestEarlyServerAcceptanceUpstream(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", v), func(t *testing.T) {
			ct, st := tlsConfigs(t)
			cache := &observedSessionCache{ClientSessionCache: tls.NewLRUClientSessionCache(4), stored: make(chan bool, 8)}
			ct.ClientSessionCache = cache
			l, err := quic.ListenAddrEarly("127.0.0.1:0", st, &quic.Config{Allow0RTT: true, Versions: []quic.Version{v}})
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
					t.Fatal("ticket disabled early data")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			udp, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			gate := &earlyPacketGate{PacketConn: udp, seen: make(chan struct{}), done: make(chan struct{})}
			release := make(chan struct{})
			tr := &upstream.Transport{Conn: &heldServerFlight{earlyPacketGate: gate, release: release}}
			defer func() { gate.Close(); tr.Close() }()
			stop := context.AfterFunc(ctx, func() { gate.Close() })
			defer stop()
			c, err := tr.DialEarly(ctx, l.Addr(), ct, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer c.CloseWithError(0, "")
			tx, err := c.OpenUniStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Write([]byte("independent early server")); err != nil {
				t.Fatal(err)
			}
			if err := tx.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-gate.seen:
			case <-ctx.Done():
				t.Fatal("no early wire packet", ctx.Err())
			}
			peer, err := l.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-peer.HandshakeComplete():
				t.Fatal("early listener waited for Finished")
			default:
			}
			rx, err := peer.AcceptUniStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			rx.SetReadDeadline(time.Now().Add(time.Second))
			b, err := io.ReadAll(rx)
			if err != nil || string(b) != "independent early server" {
				t.Fatal(string(b), err)
			}
			reply, err := peer.OpenUniStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := reply.Write([]byte("server before Finished")); err != nil {
				t.Fatal(err)
			}
			if err := reply.Close(); err != nil {
				t.Fatal(err)
			}
			close(release)
			response, err := c.AcceptUniStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			response.SetReadDeadline(time.Now().Add(time.Second))
			answer, err := io.ReadAll(response)
			if err != nil || string(answer) != "server before Finished" {
				t.Fatal(string(answer), err)
			}
			select {
			case <-c.HandshakeComplete():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case <-peer.HandshakeComplete():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if !c.ConnectionState().Used0RTT || !peer.ConnectionState().Used0RTT {
				t.Fatal("early data not accepted")
			}
		})
	}
}
