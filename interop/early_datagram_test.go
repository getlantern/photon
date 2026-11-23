package interop

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/wire"
	upstream "github.com/quic-go/quic-go"
)

// Observe (and optionally discard) actual early packets, preserving any other
// packets coalesced in the same UDP datagram. Only the reader accesses count.
type earlyDatagramObserver struct {
	net.PacketConn
	seen  chan struct{}
	drop  bool
	count int
}

func (o *earlyDatagramObserver) ReadFrom(b []byte) (int, net.Addr, error) {
	for {
		n, a, err := o.PacketConn.ReadFrom(b)
		if err != nil {
			return n, a, err
		}
		kept := 0
		for rest := b[:n]; len(rest) > 0; {
			h, e := wire.ParseHeader(rest, 8)
			if e != nil || h.PacketLen <= 0 {
				kept += copy(b[kept:], rest)
				break
			}
			packet := rest[:h.PacketLen]
			rest = rest[h.PacketLen:]
			if h.Type == wire.ZeroRTT {
				o.count++
				if o.count == 3 {
					close(o.seen)
				}
				if o.drop {
					continue
				}
			}
			kept += copy(b[kept:], packet)
		}
		if kept > 0 {
			return kept, a, nil
		}
	}
}

func waitEarlyTicket(t *testing.T, ctx context.Context, cache *observedSessionCache) {
	t.Helper()
	select {
	case enabled := <-cache.stored:
		if !enabled {
			t.Fatal("ticket disallows early data")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
func waitEarlyHandshake(t *testing.T, ctx context.Context, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
func checkDatagram(t *testing.T, ctx context.Context, receive func(context.Context) ([]byte, error), want []byte) {
	t.Helper()
	got, err := receive(ctx)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("datagram: got %d bytes, want %d, error %v", len(got), len(want), err)
	}
}

func TestEarlyClientDatagramsUpstream(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, mode := range []string{"accept", "reject", "loss"} {
			t.Run(fmt.Sprintf("%x/%s", v, mode), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
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
				for round := 0; round < 2; round++ {
					socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					observed := &earlyDatagramObserver{PacketConn: socket, seen: make(chan struct{}), drop: mode == "loss"}
					tr := &upstream.Transport{Conn: observed}
					t.Cleanup(func() { socket.Close(); tr.Close() })
					l, err := tr.ListenEarly(st, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}, EnableDatagrams: true, Allow0RTT: round == 0 || mode != "reject"})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { l.Close() })
					c, err := quic.DialAddrEarly(ctx, l.Addr().String(), ct, &quic.Config{Versions: []quic.Version{v}, EnableDatagrams: true})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { c.CloseWithError(0, "") })
					payloads := [][]byte{nil, []byte("owned early datagram"), bytes.Repeat([]byte{0xa5}, 1130)}
					if round == 1 {
						select {
						case <-c.HandshakeComplete():
							t.Fatal("not early")
						default:
						}
						for _, p := range payloads {
							owned := append([]byte(nil), p...)
							if err := c.SendDatagram(owned); err != nil {
								t.Fatal(err)
							}
							for i := range owned {
								owned[i] ^= 0xff
							}
						}
						select {
						case <-observed.seen:
						case <-ctx.Done():
							t.Fatal("missing early packets", ctx.Err())
						}
						release()
					}
					peer, err := l.Accept(ctx)
					if err != nil {
						t.Fatal(err)
					}
					waitEarlyHandshake(t, ctx, c.HandshakeComplete())
					if round == 0 {
						waitEarlyTicket(t, ctx, cache)
						continue
					}
					if c.ConnectionState().Used0RTT != (mode != "reject") {
						t.Fatal("early state")
					}
					if mode == "accept" {
						remaining := make(map[string]int)
						for _, p := range payloads {
							remaining[string(p)]++
						}
						for range payloads {
							got, err := peer.ReceiveDatagram(ctx)
							if err != nil {
								t.Fatal(err)
							}
							if remaining[string(got)] == 0 {
								t.Fatalf("unexpected or duplicate early payload (%d bytes)", len(got))
							}
							remaining[string(got)]--
						}
					}
					fresh := []byte("fresh after handshake")
					if err := c.SendDatagram(fresh); err != nil {
						t.Fatal(err)
					}
					checkDatagram(t, ctx, peer.ReceiveDatagram, fresh)
					if err := peer.SendDatagram([]byte("response")); err != nil {
						t.Fatal(err)
					}
					checkDatagram(t, ctx, c.ReceiveDatagram, []byte("response"))
				}
			})
		}
	}
}

func TestEarlyServerDatagramsUpstream(t *testing.T) { testEarlyServerDatagramsUpstream(t, "") }

func TestEarlyServerDatagramsSelectedConfig(t *testing.T) {
	testEarlyServerDatagramsUpstream(t, "policy")
}

func TestEarlyServerSelectedReducedLimits(t *testing.T) {
	for _, limit := range []string{"connection-window", "stream-window", "bidi-count", "uni-count"} {
		t.Run(limit, func(t *testing.T) { testEarlyServerDatagramsUpstream(t, limit) })
	}
}

func testEarlyServerDatagramsUpstream(t *testing.T, selection string) {
	selectQUICConfig := selection != ""
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, accept := range []bool{true, false} {
			t.Run(fmt.Sprintf("%x/accept=%v", v, accept), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				ct, st := tlsConfigs(t)
				// The static-policy fixture rejects the second ticket at TLS. Selected
				// policy/limit fixtures preserve TLS resumption. All retain valid address
				// evidence so Retry cannot account for discarded early data.
				var hellos atomic.Int32
				if !selectQUICConfig {
					st.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
						if hellos.Add(1) > 1 && !accept {
							selected := st.Clone()
							selected.GetConfigForClient = nil
							selected.SessionTicketsDisabled = true
							return selected, nil
						}
						return nil, nil
					}
				}
				cache := &observedSessionCache{ClientSessionCache: tls.NewLRUClientSessionCache(4), stored: make(chan bool, 8)}
				ct.ClientSessionCache = cache
				tokens := &observedTokenStore{TokenStore: upstream.NewLRUTokenStore(4, 4), stored: make(chan struct{}, 4)}
				cfg := &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}, EnableDatagrams: true, TokenStore: tokens}
				serverConfig := &quic.Config{Versions: []quic.Version{v}, EnableDatagrams: true, Allow0RTT: true}
				if selectQUICConfig {
					// Policy-only variants oppose listener defaults. Limit variants
					// leave selected early-data permission enabled while reducing credit.
					serverConfig.Allow0RTT = !accept
					serverConfig.EnableDatagrams = false
					serverConfig.GetConfigForClient = func(info *quic.ClientInfo) (*quic.Config, error) {
						if !info.AddrVerified {
							return nil, fmt.Errorf("unverified selection")
						}
						seed := hellos.Add(1) == 1
						selected := &quic.Config{EnableDatagrams: true, Allow0RTT: true}
						if !seed && !accept {
							switch selection {
							case "policy":
								selected.Allow0RTT = false
							case "connection-window":
								selected.InitialConnectionReceiveWindow = 1024
							case "stream-window":
								selected.InitialStreamReceiveWindow = 1024
							case "bidi-count":
								selected.MaxIncomingStreams = 1
							case "uni-count":
								selected.MaxIncomingUniStreams = 1
							default:
								return nil, fmt.Errorf("unknown selection %q", selection)
							}
						}
						return selected, nil
					}
				}
				seedListener, err := quic.ListenAddrEarly("127.0.0.1:0", st, serverConfig)
				if err != nil {
					t.Fatal(err)
				}
				defer seedListener.Close()
				seed, err := dialUpstreamAddr(t, ctx, seedListener.Addr().String(), ct, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer seed.CloseWithError(0, "")
				waitEarlyTicket(t, ctx, cache)
				if _, err := seedListener.Accept(ctx); err != nil {
					t.Fatal(err)
				}
				select {
				case <-tokens.stored:
				case <-ctx.Done():
					t.Fatal("missing address token", ctx.Err())
				}
				l := seedListener
				socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				gate := &earlyPacketGate{PacketConn: socket, seen: make(chan struct{}), done: make(chan struct{})}
				release := make(chan struct{})
				flight := &heldServerFlight{earlyPacketGate: gate, release: release}
				tr := &upstream.Transport{Conn: flight}
				defer func() { gate.Close(); tr.Close() }()
				stop := context.AfterFunc(ctx, func() { gate.Close() })
				defer stop()
				c, err := tr.DialEarly(ctx, l.Addr(), ct, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer c.CloseWithError(0, "")
				if err := c.SendDatagram([]byte("upstream early datagram")); err != nil {
					t.Fatal(err)
				}
				select {
				case <-gate.seen:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				peer, err := l.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if flight.retries.Load() != 0 {
					t.Fatal("address token did not avoid Retry")
				}
				select {
				case <-peer.HandshakeComplete():
					t.Fatal("premature handshake")
				default:
				}
				if accept {
					checkDatagram(t, ctx, peer.ReceiveDatagram, []byte("upstream early datagram"))
				}
				if err := peer.SendDatagram([]byte("server before Finished")); err != nil {
					t.Fatal(err)
				}
				close(release)
				waitEarlyHandshake(t, ctx, c.HandshakeComplete())
				waitEarlyHandshake(t, ctx, peer.HandshakeComplete())
				checkDatagram(t, ctx, c.ReceiveDatagram, []byte("server before Finished"))
				if selectQUICConfig && (!c.ConnectionState().TLS.DidResume || !peer.ConnectionState().TLS.DidResume) {
					t.Fatal("QUIC early rejection disabled TLS resumption")
				}
				if c.ConnectionState().Used0RTT != accept || peer.ConnectionState().Used0RTT != accept {
					t.Fatal("early acceptance state")
				}
				if err := c.SendDatagram([]byte("fresh upstream datagram")); err != nil {
					t.Fatal(err)
				}
				checkDatagram(t, ctx, peer.ReceiveDatagram, []byte("fresh upstream datagram"))
				if selectQUICConfig && selection != "policy" {
					if !accept {
						if _, err := c.NextConnection(ctx); err != nil {
							t.Fatal(err)
						}
					}
					tx, err := c.OpenUniStreamSync(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if !accept && selection == "uni-count" {
						if _, err := c.OpenUniStream(); err == nil {
							t.Fatal("reduced unidirectional limit not applied")
						}
					}
					if !accept && selection == "bidi-count" {
						if _, err := c.OpenStream(); err != nil {
							t.Fatal(err)
						}
						if _, err := c.OpenStream(); err == nil {
							t.Fatal("reduced bidirectional limit not applied")
						}
					}
					payload := bytes.Repeat([]byte{0x5a}, 4096)
					written := make(chan error, 1)
					deadline, _ := ctx.Deadline()
					tx.SetWriteDeadline(deadline)
					go func() {
						_, err := tx.Write(payload)
						if err == nil {
							err = tx.Close()
						}
						written <- err
					}()
					rx, err := peer.AcceptUniStream(ctx)
					if err != nil {
						t.Fatal(err)
					}
					rx.SetReadDeadline(deadline)
					got, err := io.ReadAll(rx)
					if err != nil || !bytes.Equal(got, payload) {
						t.Fatal("fresh STREAM under selected limits", len(got), err)
					}
					if err := <-written; err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}
