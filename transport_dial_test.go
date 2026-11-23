package quic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func TestTransportSharedDialAndListen(t *testing.T) { testTransportSharedDialAndListen(t, "udp") }
func TestTransportPacketConnSharedDialAndListen(t *testing.T) {
	testTransportSharedDialAndListen(t, "plain")
}

func testTransportSharedDialAndListen(t *testing.T, wrapperKind string) {
	for _, version := range []Version{Version1, Version2} {
		for _, mode := range []string{"regular", "early", "multipath"} {
			t.Run(fmt.Sprintf("%v/%s", version, mode), func(t *testing.T) {
				ct, st := dialTLS(t)
				cfg := &Config{Versions: []Version{version}, MaxConnections: 4, EnableMultipath: mode == "multipath"}
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer cancel()
				makeTransport := func() *Transport {
					socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
					if err != nil {
						t.Fatal(err)
					}
					tr := &Transport{Conn: socket}
					tr.Conn = transportTestWrapSocket(socket, wrapperKind)
					t.Cleanup(func() { tr.Close(); socket.Close() })
					return tr
				}
				a, b := makeTransport(), makeTransport()
				la, err := a.Listen(st, cfg)
				if err != nil {
					t.Fatal(err)
				}
				lb, err := b.Listen(st, cfg)
				if err != nil {
					t.Fatal(err)
				}
				dial := func(tr *Transport, addr net.Addr) *Conn {
					var c *Conn
					var err error
					if mode == "early" {
						c, err = tr.DialEarly(ctx, addr, ct, cfg)
					} else {
						c, err = tr.Dial(ctx, addr, ct, cfg)
					}
					if err != nil {
						t.Fatal(err)
					}
					if c.LocalAddr().String() != tr.Conn.LocalAddr().String() {
						t.Fatal("dial allocated another socket")
					}
					return c
				}
				var senders, receivers []*Conn
				add := func(tr *Transport, l *Listener) {
					c := dial(tr, l.Addr())
					s, err := l.Accept(ctx)
					if err != nil {
						t.Fatal(err)
					}
					senders = append(senders, c)
					receivers = append(receivers, s)
				}
				add(a, lb)
				add(b, la)
				add(a, lb)
				// One admission slot remains. Two canceled attempts must each return it.
				silent, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				defer silent.Close()
				for i := 0; i < 2; i++ {
					attempt, stop := context.WithTimeout(ctx, 30*time.Millisecond)
					_, err := a.Dial(attempt, silent.LocalAddr(), ct, cfg)
					stop()
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatal("canceled shared dial", err)
					}
				}
				if err := la.Close(); err != nil {
					t.Fatal(err)
				}
				add(a, lb) // Dialing remains available after closing the local listener.
				for i, sender := range senders {
					receiver := receivers[i]
					if mode == "multipath" && (!sender.ConnectionState().SupportsMultipath || !receiver.ConnectionState().SupportsMultipath) {
						t.Fatal("multipath not negotiated")
					}
					payload := fmt.Sprintf("shared socket transfer %d", i)
					tx, err := sender.OpenUniStreamSync(ctx)
					if err != nil {
						t.Fatal(err)
					}
					tx.SetWriteDeadline(time.Now().Add(time.Second))
					if _, err := tx.Write([]byte(payload)); err != nil {
						t.Fatal(err)
					}
					if err := tx.Close(); err != nil {
						t.Fatal(err)
					}
					rx, err := receiver.AcceptUniStream(ctx)
					if err != nil {
						t.Fatal(err)
					}
					rx.SetReadDeadline(time.Now().Add(time.Second))
					got, err := io.ReadAll(rx)
					if err != nil || string(got) != payload {
						t.Fatal("shared stream", string(got), err)
					}
				}
				canceled, stop := context.WithCancel(ctx)
				stop()
				if _, err := a.Dial(canceled, lb.Addr(), ct, cfg); !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				if err := a.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := a.Dial(ctx, lb.Addr(), ct, cfg); !errors.Is(err, ErrTransportClosed) {
					t.Fatal("dial after close", err)
				}
			})
		}
	}
}

func TestTransportVersionFallback(t *testing.T) {
	for _, selected := range []Version{Version1, Version2} {
		for _, fallback := range []bool{true, false} {
			t.Run(fmt.Sprintf("%v/fallback=%v", selected, fallback), func(t *testing.T) {
				ct, st := dialTLS(t)
				l, err := ListenAddr("127.0.0.1:0", st, &Config{Versions: []Version{selected}})
				if err != nil {
					t.Fatal(err)
				}
				defer l.Close()
				socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				defer socket.Close()
				tr := &Transport{Conn: socket, DisableVersionNegotiationPackets: true}
				defer tr.Close()
				initial := Version1
				if selected == initial {
					initial = Version2
				}
				versions := []Version{initial}
				if fallback {
					versions = append(versions, selected)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer cancel()
				c, err := tr.Dial(ctx, l.Addr(), ct, &Config{Versions: versions, MaxConnections: 1})
				if !fallback {
					var rejected *VersionNegotiationError
					if !errors.As(err, &rejected) {
						t.Fatal("expected version rejection", err)
					}
					c, err = tr.Dial(ctx, l.Addr(), ct, &Config{Versions: []Version{selected}, MaxConnections: 1})
				}
				if err != nil {
					t.Fatal(err)
				}
				if c.ConnectionState().Version != selected {
					t.Fatal("wrong selected version")
				}
				if _, err := l.Accept(ctx); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestTransportOOBSharedDialAndListen(t *testing.T) { testTransportSharedDialAndListen(t, "oob") }
