package interop

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	engine "github.com/getlantern/photon/internal/transport"
	"github.com/getlantern/photon/internal/wire"
	upstream "github.com/quic-go/quic-go"
)

// With a zero reliable boundary, the only early application frame is the reset.
// Hold the reference TLS callback until that packet arrives to prove the reset
// crossed the wire in 0-RTT, rather than being delayed until application keys.
func TestEarlyPartialResetEngineClientUpstream(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, mode := range []string{"accept", "reject", "withdrawn"} {
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
					arrival := &earlyArrivalObserver{PacketConn: socket, seen: make(chan struct{})}
					tr := &upstream.Transport{Conn: arrival}
					t.Cleanup(func() { socket.Close(); tr.Close() })
					l, err := tr.ListenEarly(st, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}, Allow0RTT: round == 0 || mode != "reject", EnableStreamResetPartialDelivery: round == 0 || mode != "withdrawn"})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { l.Close() })
					p := wire.DefaultTransportParameters()
					c, err := engine.NewConn(engine.Config{Version: v, TLS: ct, Parameters: p, EnableEarlyData: true, LocalCID: []byte("client00"), PeerCID: []byte("original"), OriginalDCID: []byte("original")})
					if err != nil {
						t.Fatal(err)
					}
					if err := c.Start(ctx); err != nil {
						c.Close()
						t.Fatal(err)
					}
					ep, err := engine.NewClientEndpoint(c, l.Addr().(*net.UDPAddr).AddrPort())
					if err != nil {
						c.Close()
						t.Fatal(err)
					}
					cs, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
					if err != nil {
						ep.Close()
						t.Fatal(err)
					}
					runtime, err := engine.NewUDPServer(cs, ep)
					if err != nil {
						ep.Close()
						cs.Close()
						t.Fatal(err)
					}
					t.Cleanup(func() { runtime.Close() })
					if err := runtime.Wait(ctx, func(*engine.ServerEndpoint) (bool, error) { return c.HandshakeConfirmed() || c.EarlyReady(), c.Err() }); err != nil {
						t.Fatal(err)
					}
					var id uint64
					released := false
					var releaseErr error
					if err := runtime.Do(ctx, func(*engine.ServerEndpoint) error {
						if round == 1 && !c.EarlyReady() {
							return errors.New("not early")
						}
						var err error
						id, err = c.OpenStream(true)
						if err != nil {
							return err
						}
						if err := c.AutoReleaseStream(id, func(err error) { released = true; releaseErr = err }); err != nil {
							return err
						}
						if round == 0 {
							_, err = c.WriteStream(id, []byte("warmup"), true)
							return err
						}
						data := bytes.Repeat([]byte("discarded"), 500)
						for offset := 0; offset < len(data); {
							n, err := c.WriteStream(id, data[offset:], false)
							if err != nil {
								return err
							}
							if n == 0 {
								return io.ErrNoProgress
							}
							offset += n
						}
						return c.CancelWriteAt(id, 7, 0)
					}); err != nil {
						t.Fatal(err)
					}
					if round == 1 {
						select {
						case <-arrival.seen:
						case <-ctx.Done():
							t.Fatal("early reset did not reach peer", ctx.Err())
						}
						release()
					}
					peer, err := l.Accept(ctx)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { peer.CloseWithError(0, "") })
					if err := runtime.Wait(ctx, func(*engine.ServerEndpoint) (bool, error) { return c.HandshakeConfirmed(), c.Err() }); err != nil {
						t.Fatal(err)
					}
					accepted := round == 1 && mode == "accept"
					if err := runtime.Do(ctx, func(*engine.ServerEndpoint) error {
						if c.EarlyDataAccepted() != accepted || c.ConnectionState().DidResume != (round == 1) {
							return errors.New("resumption outcome")
						}
						if round == 1 && !accepted {
							if !c.EarlyDataRejected() || !released || !errors.Is(releaseErr, engine.ErrEarlyDataRejected) {
								return errors.New("rejection lifecycle")
							}
							fresh, err := c.OpenStream(true)
							if err != nil {
								return err
							}
							if fresh != id {
								return errors.New("stream ID not reused")
							}
							_, err = c.WriteStream(fresh, []byte("fresh"), true)
							return err
						}
						return nil
					}); err != nil {
						t.Fatal(err)
					}
					recv, err := peer.AcceptUniStream(ctx)
					if err != nil {
						t.Fatal(err)
					}
					deadline, _ := ctx.Deadline()
					recv.SetReadDeadline(deadline)
					got, err := io.ReadAll(recv)
					if accepted {
						var reset *upstream.StreamError
						if len(got) != 0 || !errors.As(err, &reset) || reset.ErrorCode != 7 || !reset.Remote {
							t.Fatal("partial reset result", len(got), err)
						}
						if err := runtime.Wait(ctx, func(*engine.ServerEndpoint) (bool, error) { return released, releaseErr }); err != nil {
							t.Fatal("reset retirement", err)
						}
					} else {
						want := "warmup"
						if round == 1 {
							want = "fresh"
						}
						if err != nil || string(got) != want {
							t.Fatal("stream payload", string(got), err)
						}
					}
					if peer.ConnectionState().Used0RTT != accepted {
						t.Fatal("peer early outcome")
					}
					if round == 0 {
						select {
						case enabled := <-cache.stored:
							if !enabled {
								t.Fatal("ticket lacks early permission")
							}
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
					}
					runtime.Close()
					peer.CloseWithError(0, "")
					l.Close()
				}
			})
		}
	}
}
