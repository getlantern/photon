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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPublicEarlyPartialResetUpstream(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, mode := range []string{"accept", "loss", "reject", "withdrawn"} {
			accept := mode == "accept" || mode == "loss"
			t.Run(fmt.Sprintf("%x/%s", v, mode), func(t *testing.T) {
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
					dropper := &dropFirstEarlyResetPacket{PacketConn: socket, enabled: round == 1 && mode == "loss"}
					arrival := &earlyArrivalObserver{PacketConn: dropper, seen: make(chan struct{})}
					tr := &upstream.Transport{Conn: arrival}
					t.Cleanup(func() { socket.Close(); tr.Close() })
					l, err := tr.ListenEarly(st, &upstream.Config{Allow0RTT: round == 0 || mode != "reject", EnableStreamResetPartialDelivery: round == 0 || mode != "withdrawn", Versions: []upstream.Version{upstream.Version(v)}})
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
					prefix := strings.Repeat("reliable early prefix", 100)
					if _, err := tx.Write([]byte(prefix)); err != nil {
						t.Fatal(err)
					}
					if round == 0 {
						if err := tx.Close(); err != nil {
							t.Fatal(err)
						}
					} else {
						if state := c.ConnectionState().SupportsStreamResetPartialDelivery; state.Local || !state.Remote {
							t.Fatal("remembered capability", state)
						}
						tx.SetReliableBoundary()
						tx.CancelWrite(7)
						tx.SetReliableBoundary()
						tx.CancelWrite(99)
					}
					if round == 1 {
						select {
						case <-arrival.seen:
						case <-ctx.Done():
							t.Fatal("no early client wire packet", ctx.Err())
						}
						release()
					}
					expected := prefix
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
							t.Fatal("stale partial-reset handle", err)
						}
						fresh, err := c.OpenUniStreamSync(ctx)
						if err != nil {
							t.Fatal(err)
						}
						if fresh.StreamID() != tx.StreamID() {
							t.Fatal("stream ID not reset")
						}
						tx.SetReliableBoundary()
						tx.CancelWrite(42)
						if _, err := tx.Write([]byte("stale")); !errors.Is(err, quic.Err0RTTRejected) {
							t.Fatal("stale write", err)
						}
						if err := tx.Close(); !errors.Is(err, quic.Err0RTTRejected) {
							t.Fatal("stale close", err)
						}
						if err := tx.Release(ctx); !errors.Is(err, quic.Err0RTTRejected) {
							t.Fatal("stale release", err)
						}
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
					if round == 1 && accept {
						var reset *upstream.StreamError
						if !errors.As(err, &reset) || reset.ErrorCode != 7 || !reset.Remote || string(b) != expected {
							t.Fatal("early partial delivery", len(b), err)
						}
						if err := tx.Release(ctx); err != nil {
							t.Fatal("release", err)
						}
						var ownReset *quic.StreamError
						if err := tx.WaitAcknowledged(ctx); !errors.As(err, &ownReset) || ownReset.ErrorCode != 7 || ownReset.Remote {
							t.Fatal("local delivery result", err)
						}
					} else if err != nil || string(b) != expected {
						t.Fatal(string(b), err)
					}
					select {
					case <-c.HandshakeComplete():
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					if state := c.ConnectionState(); state.TLS.DidResume != (round == 1) || state.SupportsStreamResetPartialDelivery.Remote != (round == 0 || mode != "withdrawn") {
						t.Fatal("resumed capability state", state)
					}
					if round == 1 && mode == "loss" && !dropper.dropped.Load() {
						t.Fatal("loss fixture did not drop an early packet")
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

// Drop the first early packet, which contains prefix data. A successful reliable
// read then requires recovery after cancellation, not mere delivery before reset.
type dropFirstEarlyResetPacket struct {
	net.PacketConn
	enabled bool
	dropped atomic.Bool
}

func (d *dropFirstEarlyResetPacket) ReadFrom(b []byte) (int, net.Addr, error) {
	for {
		n, addr, err := d.PacketConn.ReadFrom(b)
		if err == nil && d.enabled {
			h, parseErr := wire.ParseHeader(b[:n], 0)
			if parseErr == nil && h.Type == wire.ZeroRTT && d.dropped.CompareAndSwap(false, true) {
				continue
			}
		}
		return n, addr, err
	}
}
