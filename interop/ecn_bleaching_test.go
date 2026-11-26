//go:build linux || darwin

package interop

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
	"golang.org/x/sys/unix"
)

// A real UDP relay strips markings in both directions without parsing or
// modifying protected QUIC bytes. Upstream keeps its native ECN-capable socket.
func TestECNSenderStopsAfterBleaching(t *testing.T) { runECNRelayFallback(t, false) }

func TestECNSenderSurvivesMarkedPacketLoss(t *testing.T) { runECNRelayFallback(t, true) }

func runECNRelayFallback(t *testing.T, drop bool) {
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
				addr, err := net.ResolveUDPAddr("udp", address)
				if err != nil {
					t.Fatal(err)
				}
				socket, err := net.ListenUDP("udp", addr)
				if err != nil {
					t.Fatal(err)
				}
				defer socket.Close()
				raw, err := socket.SyscallConn()
				if err != nil {
					t.Fatal(err)
				}
				var optionErr error
				err = raw.Control(func(fd uintptr) {
					level, option := unix.IPPROTO_IP, unix.IP_RECVTOS
					if addr.IP.To4() == nil {
						level, option = unix.IPPROTO_IPV6, unix.IPV6_RECVTCLASS
					}
					optionErr = unix.SetsockoptInt(int(fd), level, option, 1)
				})
				if err != nil || optionErr != nil {
					t.Fatal(err, optionErr)
				}
				observer := &ecnBlindConn{PacketConn: socket, socket: socket}
				target := listener.Addr().(*net.UDPAddr)
				marks := make(chan uint8, 1024)
				done := make(chan error, 1)
				go func() {
					var buffer [65535]byte
					var client net.Addr
					for {
						n, from, err := observer.ReadFrom(buffer[:])
						if err != nil {
							done <- err
							return
						}
						destination := net.Addr(target)
						if from.String() == target.String() {
							if client == nil {
								continue
							}
							destination = client
						} else {
							client = from
							observer.mu.Lock()
							mark := observer.marks[len(observer.marks)-1]
							observer.mu.Unlock()
							select {
							case marks <- mark:
							default:
								done <- fmt.Errorf("relay observation overflow")
								return
							}
							if drop && mark == 2 {
								continue
							}
						}
						// WriteTo uses the default Not-ECT socket traffic class.
						if _, err := socket.WriteTo(buffer[:n], destination); err != nil {
							done <- err
							return
						}
					}
				}()
				defer func() { socket.Close(); <-done }()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				c, err := quic.DialAddr(ctx, socket.LocalAddr().String(), ct, &quic.Config{Versions: []quic.Version{v}, EnableDatagrams: true})
				if err != nil {
					t.Fatal(err)
				}
				defer c.CloseWithError(0, "")
				peer, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer peer.CloseWithError(0, "")
				for i := 0; i < 24; i++ {
					payload := fmt.Sprintf("bleached data %d", i)
					if err := c.SendDatagram([]byte(payload)); err != nil {
						t.Fatal(err)
					}
					b, err := peer.ReceiveDatagram(ctx)
					if err != nil || string(b) != payload {
						t.Fatal(string(b), err)
					}
					select {
					case mark := <-trace.datagrams:
						// Upstream v0.62.0 toQlogECN maps Not-ECT to ECNUnsupported.
						if mark != qlog.ECNUnsupported {
							t.Fatal("relay did not bleach", mark)
						}
					case <-ctx.Done():
						t.Fatal("no upstream ECN observation", ctx.Err())
					}
					if err := peer.SendDatagram(b); err != nil {
						t.Fatal(err)
					}
					b, err = c.ReceiveDatagram(ctx)
					if err != nil || string(b) != payload {
						t.Fatal(string(b), err)
					}
				}
				var observed []uint8
				for len(marks) > 0 {
					observed = append(observed, <-marks)
				}
				marked := false
				for _, mark := range observed {
					if mark == 2 {
						marked = true
					}
				}
				if !marked || len(observed) < 24 {
					t.Fatal("no initial marked probes", observed)
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
