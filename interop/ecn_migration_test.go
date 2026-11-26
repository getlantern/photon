//go:build linux || darwin

package interop

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
	"golang.org/x/sys/unix"
)

func TestECNSenderRevalidatesAfterMigrationUpstream(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, address := range []string{"127.0.0.1:0", "[::1]:0"} {
			t.Run(fmt.Sprintf("%x/%s", v, address), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				trace := &ecnSenderTrace{datagrams: make(chan qlog.ECN, 64)}
				listener, err := upstream.ListenAddr(address, st, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}, EnableDatagrams: true, Tracer: func(context.Context, bool, upstream.ConnectionID) qlogwriter.Trace { return trace }})
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				local, err := net.ResolveUDPAddr("udp", address)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				c, err := quic.DialAddr(ctx, listener.Addr().String(), ct, &quic.Config{Versions: []quic.Version{v}, EnableDatagrams: true, CongestionControl: quic.CongestionControlNewReno, LocalAddr: local})
				if err != nil {
					t.Fatal(err)
				}
				defer c.CloseWithError(0, "")
				peer, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer peer.CloseWithError(0, "")
				packets := 24
				previous := c.LocalAddr().String()
				for round := 0; round < 3; round++ {
					if round > 0 {
						if err := c.Migrate(ctx, local); err != nil {
							t.Fatal("migration", round, err)
						}
						if c.LocalAddr().String() == previous {
							t.Fatal("migration kept local tuple")
						}
						previous = c.LocalAddr().String()
					}
					marked := 0
					for i := 0; i < packets; i++ {
						payload := fmt.Sprintf("ECN migration %d/%d", round, i)
						if err := c.SendDatagram([]byte(payload)); err != nil {
							t.Fatal(err)
						}
						b, err := peer.ReceiveDatagram(ctx)
						if err != nil || string(b) != payload {
							t.Fatalf("round=%d packet=%d upstreamKeyPhase=%d data=%q err=%v", round, i, trace.keyPhase.Load(), b, err)
						}
						select {
						case mark := <-trace.datagrams:
							if mark == qlog.ECT0 {
								marked++
							} else if mark != qlog.ECNUnsupported && mark != qlog.ECTNot {
								t.Fatal("unexpected ECN", mark)
							}
						case <-ctx.Done():
							t.Fatal("no marking observation", ctx.Err())
						}
						if err := peer.SendDatagram(b); err != nil {
							t.Fatal(err)
						}
						b, err = c.ReceiveDatagram(ctx)
						if err != nil || string(b) != payload {
							t.Fatalf("round=%d packet=%d upstreamKeyPhase=%d data=%q err=%v", round, i, trace.keyPhase.Load(), b, err)
						}
					}
					if marked <= 10 {
						t.Fatalf("round %d did not validate ECN: %d/%d marked", round, marked, packets)
					}
					if peer.RemoteAddr().String() != previous {
						t.Fatal("peer did not use migrated tuple", peer.RemoteAddr(), previous)
					}
				}
			})
		}
	}
}

// ectDroppingRelay gives each client address its own server-facing socket, so
// the peer observes the move. It preserves markings from the first client
// address, so that path validates as ECN-capable, and drops every marked packet
// from any later address. Only a sender that restarts its bounded test on the
// new path gets through: inherited capability keeps marking and is never
// acknowledged.
type ectDroppingRelay struct {
	front    *net.UDPConn
	done     chan error
	readers  sync.WaitGroup
	mu       sync.Mutex
	backs    []*net.UDPConn
	migrated []uint8 // markings from later client addresses, dropped or not
}

func newECTDroppingRelay(t *testing.T, address string, target *net.UDPAddr) *ectDroppingRelay {
	t.Helper()
	addr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		t.Fatal(err)
	}
	front, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	level, receive, send := unix.IPPROTO_IP, unix.IP_RECVTOS, unix.IP_TOS
	if addr.IP.To4() == nil {
		level, receive, send = unix.IPPROTO_IPV6, unix.IPV6_RECVTCLASS, unix.IPV6_TCLASS
	}
	setOption := func(socket *net.UDPConn, option, value int) (optionErr error) {
		raw, err := socket.SyscallConn()
		if err != nil {
			return err
		}
		if err := raw.Control(func(fd uintptr) { optionErr = unix.SetsockoptInt(int(fd), level, option, value) }); err != nil {
			return err
		}
		return optionErr
	}
	if err := setOption(front, receive, 1); err != nil {
		front.Close()
		t.Fatal(err)
	}
	r := &ectDroppingRelay{front: front, done: make(chan error, 1)}
	observer := &ecnBlindConn{PacketConn: front, socket: front}
	go func() {
		var buffer [65535]byte
		var first string
		backs := map[string]*net.UDPConn{}
		for {
			n, client, err := observer.ReadFrom(buffer[:])
			if err != nil {
				r.done <- err
				return
			}
			observer.mu.Lock()
			mark := observer.marks[len(observer.marks)-1]
			observer.marks = observer.marks[:0]
			observer.mu.Unlock()
			key := client.String()
			if first == "" {
				first = key
			}
			if key != first {
				r.mu.Lock()
				r.migrated = append(r.migrated, mark)
				r.mu.Unlock()
				if mark != 0 {
					continue
				}
			}
			back := backs[key]
			if back == nil {
				if back, err = net.DialUDP("udp", nil, target); err != nil {
					r.done <- err
					return
				}
				backs[key] = back
				r.mu.Lock()
				r.backs = append(r.backs, back)
				r.mu.Unlock()
				r.readers.Add(1)
				go func() {
					defer r.readers.Done()
					var buffer [65535]byte
					for {
						n, err := back.Read(buffer[:])
						if err != nil {
							return
						}
						// Not-ECT: only the client's own markings are under test.
						front.WriteTo(buffer[:n], client)
					}
				}()
			}
			if err := setOption(back, send, int(mark)); err != nil {
				r.done <- err
				return
			}
			if _, err := back.Write(buffer[:n]); err != nil {
				r.done <- err
				return
			}
		}
	}()
	return r
}

func (r *ectDroppingRelay) close() {
	r.front.Close()
	<-r.done
	r.mu.Lock()
	for _, back := range r.backs {
		back.Close()
	}
	r.mu.Unlock()
	r.readers.Wait()
}

func (r *ectDroppingRelay) marks() []uint8 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint8(nil), r.migrated...)
}

// Marked packets validate the original path and are all dropped on the migrated
// one. The sender must spend no more than a fresh ten-packet test there before
// falling back to Not-ECT; the capability of the path it left proves nothing.
func TestECNSenderRetestsMigratedPathThatDropsECT(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, address := range []string{"127.0.0.1:0", "[::1]:0"} {
			t.Run(fmt.Sprintf("%x/%s", v, address), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				trace := &ecnSenderTrace{datagrams: make(chan qlog.ECN, 64)}
				listener, err := upstream.ListenAddr(address, st, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}, EnableDatagrams: true, Tracer: func(context.Context, bool, upstream.ConnectionID) qlogwriter.Trace { return trace }})
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				relay := newECTDroppingRelay(t, address, listener.Addr().(*net.UDPAddr))
				defer relay.close()
				local, err := net.ResolveUDPAddr("udp", address)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				c, err := quic.DialAddr(ctx, relay.front.LocalAddr().String(), ct, &quic.Config{Versions: []quic.Version{v}, EnableDatagrams: true, CongestionControl: quic.CongestionControlNewReno, LocalAddr: local})
				if err != nil {
					t.Fatal(err)
				}
				defer c.CloseWithError(0, "")
				peer, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer peer.CloseWithError(0, "")
				marked := 0
				for i := 0; i < 24; i++ {
					payload := fmt.Sprintf("ECN original path %d", i)
					if err := c.SendDatagram([]byte(payload)); err != nil {
						t.Fatal(err)
					}
					b, err := peer.ReceiveDatagram(ctx)
					if err != nil || string(b) != payload {
						t.Fatal(i, string(b), err)
					}
					select {
					case mark := <-trace.datagrams:
						if mark == qlog.ECT0 {
							marked++
						}
					case <-ctx.Done():
						t.Fatal("no marking observation", ctx.Err())
					}
					if err := peer.SendDatagram(b); err != nil {
						t.Fatal(err)
					}
					if b, err = c.ReceiveDatagram(ctx); err != nil || string(b) != payload {
						t.Fatal(i, string(b), err)
					}
				}
				// Otherwise the migrated path could not tell inheritance from retesting.
				if marked <= 10 {
					t.Fatalf("original path did not validate ECN through the relay: %d/24 marked", marked)
				}
				if err := c.Migrate(ctx, local); err != nil {
					t.Fatal("migration", err, relay.marks())
				}
				// Datagrams are not retransmitted, so each one that arrives
				// crossed the migrated path unmarked.
				arrived := make(chan struct{}, 1024)
				go func() {
					for {
						if _, err := peer.ReceiveDatagram(ctx); err != nil {
							return
						}
						arrived <- struct{}{}
					}
				}()
				for delivered := 0; delivered < 24; {
					if err := c.SendDatagram([]byte("ECN migrated path")); err != nil {
						t.Fatal(err)
					}
					select {
					case <-arrived:
						delivered++
					case <-time.After(20 * time.Millisecond):
					case <-ctx.Done():
						t.Fatal("migrated path never carried unmarked data", relay.marks())
					}
					if dropped := countMarked(relay.marks()); dropped > 10 {
						t.Fatalf("sender kept marking beyond a fresh probe budget: %d dropped, %v", dropped, relay.marks())
					}
				}
				observed := relay.marks()
				if countMarked(observed) == 0 {
					t.Fatal("sender did not test the migrated path", observed)
				}
				for _, mark := range observed[len(observed)-8:] {
					if mark != 0 {
						t.Fatal("sender marking did not stop", observed)
					}
				}
			})
		}
	}
}

func countMarked(marks []uint8) (n int) {
	for _, mark := range marks {
		if mark != 0 {
			n++
		}
	}
	return n
}
