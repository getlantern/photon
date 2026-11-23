package interop

import (
	"context"
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

// Suppress orderly close packets while replacing the peer runtime, emulating
// lost connection state with the same socket and stateless reset secret.
type resetRestartSocket struct {
	net.PacketConn
	muted  atomic.Bool
	writes atomic.Int64
}

func (s *resetRestartSocket) WriteTo(p []byte, a net.Addr) (int, error) {
	if s.muted.Load() {
		return len(p), nil
	}
	n, err := s.PacketConn.WriteTo(p, a)
	if err == nil {
		s.writes.Add(1)
	}
	return n, err
}

func TestStatelessResetIndependentRestart(t *testing.T) { testStatelessResetRestart(t, false, false) }
func TestStatelessResetMigratedIndependentRestart(t *testing.T) {
	testStatelessResetRestart(t, true, false)
}

func TestStatelessResetProbingIndependentRestart(t *testing.T) {
	testStatelessResetRestart(t, false, true)
}

// Once muted, neither direction on the original socket can terminate the client.
// The probing fixture must detect the reset through the alternative Transport.
type resetProbePrimarySocket struct{ resetRestartSocket }

func (s *resetProbePrimarySocket) ReadFrom(b []byte) (int, net.Addr, error) {
	for {
		n, peer, err := s.PacketConn.ReadFrom(b)
		if err != nil || !s.muted.Load() {
			return n, peer, err
		}
	}
}

func testStatelessResetRestart(t *testing.T, migrate, probe bool) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			ct, st := tlsConfigs(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			udp, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer udp.Close()
			socket := &resetRestartSocket{PacketConn: udp}
			key := upstream.StatelessResetKey{1, 2, 3, 4}
			peerTransport := &upstream.Transport{Conn: socket, StatelessResetKey: &key}
			defer peerTransport.Close()
			listener, err := peerTransport.Listen(st, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			clientConfig := &quic.Config{Versions: []quic.Version{version}}
			if migrate || probe {
				clientConfig.LocalAddr = &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}
			}
			var client *quic.Conn
			var primary *resetProbePrimarySocket
			if probe {
				clientSocket, listenErr := net.ListenPacket("udp4", "127.0.0.1:0")
				if listenErr != nil {
					t.Fatal(listenErr)
				}
				primary = &resetProbePrimarySocket{resetRestartSocket: resetRestartSocket{PacketConn: clientSocket}}
				tr := &quic.Transport{Conn: primary}
				t.Cleanup(func() { tr.Close(); clientSocket.Close() })
				client, err = tr.Dial(ctx, udp.LocalAddr(), ct, clientConfig)
			} else {
				client, err = quic.DialAddr(ctx, udp.LocalAddr().String(), ct, clientConfig)
			}
			if err != nil {
				t.Fatal(err)
			}
			defer client.CloseWithError(0, "")
			peer, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var staged *quic.Path
			if migrate || probe {
				alternate, err := net.ListenPacket("udp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				tr := &quic.Transport{Conn: alternate}
				t.Cleanup(func() { tr.Close(); alternate.Close() })
				path, err := client.AddPath(tr)
				if err != nil {
					t.Fatal(err)
				}
				staged = path
				if migrate {
					if err := path.Probe(ctx); err != nil {
						t.Fatal(err)
					}
					if err := path.Switch(); err != nil {
						t.Fatal(err)
					}
					if client.LocalAddr().String() != alternate.LocalAddr().String() {
						t.Fatal("migration did not switch source")
					}
				}
			}
			stream, err := client.OpenStream()
			if err != nil {
				t.Fatal(err)
			}
			payload := make([]byte, 128)
			if _, err = stream.Write(payload); err != nil {
				t.Fatal(err)
			}
			received, err := peer.AcceptStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			deadline, _ := ctx.Deadline()
			received.SetReadDeadline(deadline)
			if _, err = io.ReadFull(received, payload); err != nil {
				t.Fatal(err)
			}
			if primary != nil {
				primary.muted.Store(true)
			}
			socket.muted.Store(true)
			if err = peerTransport.Close(); err != nil {
				t.Fatal(err)
			}
			// Close joins the old reader and restores the caller-owned socket deadline.
			fresh := &upstream.Transport{Conn: socket, StatelessResetKey: &key}
			defer fresh.Close()
			freshListener, err := fresh.Listen(st, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}})
			if err != nil {
				t.Fatal(err)
			}
			defer freshListener.Close()
			before := socket.writes.Load()
			socket.muted.Store(false)
			var probeErr error
			if probe {
				probeErr = staged.Probe(ctx)
			} else {
				if _, err = stream.Write(payload); err != nil {
					t.Fatal("trigger reset", err)
				}
			}
			select {
			case <-client.Context().Done():
			case <-ctx.Done():
				t.Fatal("peer reset not observed", ctx.Err())
			}
			assertReset := func(err error) {
				t.Helper()
				var reset *quic.StatelessResetError
				var network net.Error
				if !errors.As(err, &reset) || !errors.As(err, &network) || network.Timeout() || !network.Temporary() || !errors.Is(err, net.ErrClosed) {
					t.Fatal("reset contract", err)
				}
			}
			if probe {
				assertReset(probeErr)
			}
			assertReset(context.Cause(client.Context()))
			_, err = stream.Write(payload)
			assertReset(err)
			_, err = client.OpenStream()
			assertReset(err)
			if socket.writes.Load() <= before {
				t.Fatal("replacement peer emitted no packet")
			}
		})
	}
}
