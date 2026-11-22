package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/transport"
)

func TestPublicEarlyDial(t *testing.T)    { testPublicEarlyDial(t, "address") }
func TestTransportEarlyDial(t *testing.T) { testPublicEarlyDial(t, "transport") }

func TestPacketConnEarlyDial(t *testing.T) { testPublicEarlyDial(t, "packet") }

func testPublicEarlyDial(t *testing.T, mode string) {
	for _, compatible := range []bool{false, true} {
		for _, v := range []Version{Version1, Version2} {
			for _, accept := range []bool{true, false} {
				ct, st := dialTLS(t)
				ct.ClientSessionCache = tls.NewLRUClientSessionCache(4)
				dialEarly := DialAddrEarly
				if mode == "transport" {
					socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
					if err != nil {
						t.Fatal(err)
					}
					tr := &Transport{Conn: socket}
					t.Cleanup(func() { tr.Close(); socket.Close() })
					dialEarly = func(ctx context.Context, address string, tlsConfig *tls.Config, cfg *Config) (*Conn, error) {
						peer, err := net.ResolveUDPAddr("udp", address)
						if err != nil {
							return nil, err
						}
						return tr.DialEarly(ctx, peer, tlsConfig, cfg)
					}
				}
				if mode == "packet" {
					dialEarly = func(ctx context.Context, address string, tlsConfig *tls.Config, cfg *Config) (*Conn, error) {
						peer, err := net.ResolveUDPAddr("udp", address)
						if err != nil {
							return nil, err
						}
						return DialEarly(ctx, helperSocket(t), peer, tlsConfig, cfg)
					}
				}
				var hellos atomic.Int32
				release := make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				defer unblock()
				st.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
					if hellos.Add(1) > 1 {
						<-release
					}
					return nil, nil
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				first, err := ListenAddr("127.0.0.1:0", st, &Config{Allow0RTT: true, Versions: []Version{v}})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { first.Close() })
				cfg := &Config{Versions: []Version{v}}
				initial, err := dialEarly(ctx, first.Addr().String(), ct, cfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { initial.server.Close() })
				if err := initial.server.Wait(ctx, func(*transport.ServerEndpoint) (bool, error) {
					_, ok := ct.ClientSessionCache.Get("127.0.0.1")
					return ok, nil
				}); err != nil {
					t.Fatal(err)
				}
				serverConfig := &Config{Allow0RTT: accept, Versions: []Version{v}}
				selected := v
				if compatible {
					selected = Version1
					if selected == v {
						selected = Version2
					}
					cfg.EnableCompatibleVersionNegotiation = true
					cfg.Versions = []Version{selected, v}
					cfg.InitialVersion = v
					serverConfig.EnableCompatibleVersionNegotiation = true
					serverConfig.Versions = []Version{selected, v}
				}
				l, err := ListenAddr("127.0.0.1:0", st, serverConfig)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { l.Close() })
				dialContext, cancelDial := context.WithCancel(context.WithValue(ctx, transportContextKey{}, "early metadata"))
				early, err := dialEarly(dialContext, l.Addr().String(), ct, cfg)
				cancelDial()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { early.server.Close() })
				if early.Context().Value(transportContextKey{}) != "early metadata" || early.Context().Err() != nil {
					t.Fatal("lost early dial context values or retained cancellation")
				}
				select {
				case <-early.HandshakeComplete():
					t.Fatal("early dial waited for handshake")
				default:
				}
				stream, err := early.OpenStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if stream.Context().Value(transportContextKey{}) != "early metadata" {
					t.Fatal("lost early stream context value")
				}
				if _, err := stream.Write([]byte("early request")); err != nil {
					t.Fatal(err)
				}
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
				canceled, stop := context.WithCancel(ctx)
				stop()
				if _, err := early.NextConnection(canceled); !errors.Is(err, context.Canceled) {
					t.Fatal("canceled transition", err)
				}
				caused, stopCause := context.WithCancelCause(ctx)
				reason := errors.New("caller abandoned recovery")
				stopCause(reason)
				if _, err := early.NextConnection(caused); !errors.Is(err, reason) {
					t.Fatal("lost cancellation cause", err)
				}
				waiting, stopWaiting := context.WithTimeout(ctx, 10*time.Millisecond)
				if _, err := early.NextConnection(waiting); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("transition did not wait for TLS", err)
				}
				stopWaiting()
				unblock()
				select {
				case <-early.HandshakeComplete():
				case <-early.Context().Done():
					t.Fatal(context.Cause(early.Context()))
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if early.ConnectionState().Version != selected {
					t.Fatal("negotiated version", early.ConnectionState().Version)
				}
				if early.ConnectionState().Used0RTT != accept {
					t.Fatal("incorrect early-data state", accept)
				}
				peer, err := l.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if accept {
					recovered, err := early.NextConnection(ctx)
					if err != nil || recovered != early {
						t.Fatal("accepted early transition", err)
					}
				}
				expected := "early request"
				if !accept {
					if err := stream.WaitAcknowledged(ctx); !errors.Is(err, Err0RTTRejected) {
						t.Fatal("rejection lost after FIN", err)
					}
					for _, operation := range []func() error{
						func() error { _, err := early.OpenStream(); return err },
						func() error { _, err := early.OpenUniStream(); return err },
						func() error { _, err := early.OpenStreamSync(ctx); return err },
						func() error { _, err := early.OpenUniStreamSync(ctx); return err },
						func() error { _, err := early.AcceptStream(ctx); return err },
						func() error { _, err := early.AcceptUniStream(ctx); return err },
					} {
						if err := operation(); !errors.Is(err, Err0RTTRejected) {
							t.Fatal("rejection gate", err)
						}
					}
					var callers sync.WaitGroup
					for i := 0; i < 8; i++ {
						callers.Add(1)
						go func() {
							defer callers.Done()
							recovered, err := early.NextConnection(ctx)
							if err != nil || recovered != early {
								t.Error("recovery", err)
							}
						}()
					}
					callers.Wait()
					fresh, err := early.OpenStreamSync(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if fresh.StreamID() != stream.StreamID() {
						t.Fatal("stream IDs not reset")
					}
					stream.CancelWrite(19)
					stream.CancelRead(20)
					if _, err := fresh.Write([]byte("fresh request")); err != nil {
						t.Fatal(err)
					}
					if err := fresh.Close(); err != nil {
						t.Fatal(err)
					}
					expected = "fresh request"
				}
				rx, err := peer.AcceptStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				rx.SetReadDeadline(time.Now().Add(time.Second))
				got, err := io.ReadAll(rx)
				if err != nil || string(got) != expected {
					t.Fatal(string(got), err)
				}
			}
		}
	}
}
