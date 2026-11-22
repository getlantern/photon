package interop

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

func TestTransportListenerIndependentRestart(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ct, st := tlsConfigs(t)
			socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer socket.Close()
			tr := &quic.Transport{Conn: socket}
			defer tr.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			l, err := tr.Listen(st, &quic.Config{Versions: []quic.Version{version}})
			if err != nil {
				t.Fatal(err)
			}
			var own []*quic.Conn
			var peers []*upstream.Conn
			for i := 0; i < 2; i++ {
				peer, err := dialUpstreamAddr(t, ctx, l.Addr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}})
				if err != nil {
					t.Fatal(err)
				}
				defer peer.CloseWithError(0, "")
				c, err := l.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				own = append(own, c)
				peers = append(peers, peer)
				if i == 0 {
					if err := l.Close(); err != nil {
						t.Fatal(err)
					}
					old := l
					l, err = tr.Listen(st, &quic.Config{Versions: []quic.Version{version}})
					if err != nil {
						t.Fatal(err)
					}
					if err := old.Close(); err != nil {
						t.Fatal(err)
					}
				}
			}
			for i, c := range own {
				payload := bytes.Repeat([]byte(fmt.Sprintf("transport-%d", i)), 3000)
				tx, err := peers[i].OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				tx.SetWriteDeadline(time.Now().Add(2 * time.Second))
				if _, err := tx.Write(payload); err != nil {
					t.Fatal(err)
				}
				if err := tx.Close(); err != nil {
					t.Fatal(err)
				}
				rx, err := c.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				rx.SetReadDeadline(time.Now().Add(2 * time.Second))
				got, err := io.ReadAll(rx)
				if err != nil || !bytes.Equal(got, payload) {
					t.Fatal("receive after restart", len(got), err)
				}
				out, err := c.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				out.SetWriteDeadline(time.Now().Add(2 * time.Second))
				if _, err := out.Write(payload); err != nil {
					t.Fatal(err)
				}
				if err := out.Close(); err != nil {
					t.Fatal(err)
				}
				in, err := peers[i].AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				in.SetReadDeadline(time.Now().Add(2 * time.Second))
				got, err = io.ReadAll(in)
				if err != nil || !bytes.Equal(got, payload) {
					t.Fatal("send after restart", len(got), err)
				}
			}
			if err := tr.Close(); err != nil {
				t.Fatal(err)
			}
			for _, c := range own {
				select {
				case <-c.Context().Done():
				default:
					t.Fatal("transport retained accepted connection")
				}
			}
			if _, err := socket.WriteToUDP([]byte("returned"), socket.LocalAddr().(*net.UDPAddr)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTransportIndependentSimultaneousRoles(t *testing.T) {
	testTransportIndependentSimultaneousRoles(t, "udp")
}
func TestTransportPacketConnIndependentRoles(t *testing.T) {
	testTransportIndependentSimultaneousRoles(t, "plain")
}

type transportTestPacketConn struct {
	net.PacketConn
	marker []byte
}

func testTransportIndependentSimultaneousRoles(t *testing.T, wrapperKind string) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ct, st := tlsConfigs(t)
			socket := func() *net.UDPConn {
				s, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { s.Close() })
				return s
			}
			a := &quic.Transport{Conn: socket()}
			switch wrapperKind {
			case "plain":
				a.Conn = transportTestPacketConn{a.Conn, []byte{1}}
			case "oob":
				a.Conn = transportTestOOBConn{a.Conn.(quic.OOBCapablePacketConn), []byte{1}}
			}
			b := &upstream.Transport{Conn: socket()}
			defer a.Close()
			defer b.Close()
			cfg := &quic.Config{Versions: []quic.Version{version}}
			peerCfg := &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}
			la, err := a.Listen(st, cfg)
			if err != nil {
				t.Fatal(err)
			}
			lb, err := b.Listen(st, peerCfg)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			outgoing, err := a.Dial(ctx, lb.Addr(), ct, cfg)
			if err != nil {
				t.Fatal(err)
			}
			peerIncoming, err := lb.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			peerOutgoing, err := b.Dial(ctx, la.Addr(), ct, peerCfg)
			if err != nil {
				t.Fatal(err)
			}
			incoming, err := la.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if outgoing.LocalAddr().String() != la.Addr().String() || peerOutgoing.LocalAddr().String() != lb.Addr().String() {
				t.Fatal("dial used different socket")
			}
			la.Close()
			lb.Close()
			// Raw datagrams coexist with both established roles after listener
			// closure, through the same primary sockets.
			for _, read := range []func(context.Context, []byte) (int, net.Addr, error){a.ReadNonQUICPacket, b.ReadNonQUICPacket} {
				arm, stop := context.WithTimeout(ctx, 20*time.Millisecond)
				_, _, err := read(arm, make([]byte, 32))
				stop()
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("arming raw reader", err)
				}
			}
			outbound := []byte{0x3f, 1, 2, 3}
			if n, err := a.WriteTo(outbound, b.Conn.LocalAddr()); err != nil || n != len(outbound) {
				t.Fatal(n, err)
			}
			var raw [32]byte
			n, source, err := b.ReadNonQUICPacket(ctx, raw[:])
			if err != nil || !bytes.Equal(raw[:n], outbound) || source.String() != a.Conn.LocalAddr().String() {
				t.Fatal("peer raw receive", n, source, err)
			}
			inbound := []byte{0, 9, 8, 7}
			if n, err := b.WriteTo(inbound, a.Conn.LocalAddr()); err != nil || n != len(inbound) {
				t.Fatal(n, err)
			}
			n, source, err = a.ReadNonQUICPacket(ctx, raw[:])
			if err != nil || !bytes.Equal(raw[:n], inbound) || source.String() != b.Conn.LocalAddr().String() {
				t.Fatal("own raw receive", n, source, err)
			}
			for i, own := range []*quic.Conn{outgoing, incoming} {
				peer := []*upstream.Conn{peerIncoming, peerOutgoing}[i]
				payload := bytes.Repeat([]byte(fmt.Sprintf("role-%d", i)), 5000)
				tx, err := own.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				tx.SetWriteDeadline(time.Now().Add(2 * time.Second))
				if _, err := tx.Write(payload); err != nil {
					t.Fatal(err)
				}
				if err := tx.Close(); err != nil {
					t.Fatal(err)
				}
				rx, err := peer.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				rx.SetReadDeadline(time.Now().Add(2 * time.Second))
				got, err := io.ReadAll(rx)
				if err != nil || !bytes.Equal(got, payload) {
					t.Fatal("own send", len(got), err)
				}
				reply, err := peer.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				reply.SetWriteDeadline(time.Now().Add(2 * time.Second))
				if _, err := reply.Write(payload); err != nil {
					t.Fatal(err)
				}
				if err := reply.Close(); err != nil {
					t.Fatal(err)
				}
				received, err := own.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				received.SetReadDeadline(time.Now().Add(2 * time.Second))
				got, err = io.ReadAll(received)
				if err != nil || !bytes.Equal(got, payload) {
					t.Fatal("own receive", len(got), err)
				}
			}
		})
	}
}

type transportTestOOBConn struct {
	quic.OOBCapablePacketConn
	marker []byte
}

func TestTransportOOBIndependentRoles(t *testing.T) {
	testTransportIndependentSimultaneousRoles(t, "oob")
}
