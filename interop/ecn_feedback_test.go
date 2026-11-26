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

type ecnFeedbackTrace struct{ counts chan [3]uint64 }

func (t *ecnFeedbackTrace) AddProducer() qlogwriter.Recorder { return t }
func (t *ecnFeedbackTrace) SupportsSchemas(string) bool      { return true }
func (t *ecnFeedbackTrace) Close() error                     { return nil }
func (t *ecnFeedbackTrace) RecordEvent(event qlogwriter.Event) {
	p, ok := event.(qlog.PacketReceived)
	if !ok || p.Header.PacketType != qlog.PacketType1RTT {
		return
	}
	for _, f := range p.Frames {
		ack, ok := f.Frame.(*qlog.AckFrame)
		if !ok {
			continue
		}
		if ack.ECT0+ack.ECT1+ack.ECNCE == 0 {
			continue
		}
		select {
		case t.counts <- [3]uint64{ack.ECT0, ack.ECT1, ack.ECNCE}:
		default:
		}
	}
}

// Hide UDP offload methods so upstream sends through WriteTo with the socket's
// fixed marking. This tests our feedback, not upstream's sender ECN validation.
type markedPacketConn struct{ net.PacketConn }

func TestECNFeedbackUpstream(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, mark := range []int{1, 2, 3} {
			t.Run(fmt.Sprintf("%x/%d", v, mark), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				listener, err := quic.ListenAddr("127.0.0.1:0", st, &quic.Config{Versions: []quic.Version{v}, EnableDatagrams: true})
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				defer socket.Close()
				raw, err := socket.SyscallConn()
				if err != nil {
					t.Fatal(err)
				}
				var optionErr error
				err = raw.Control(func(fd uintptr) { optionErr = unix.SetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_TOS, mark) })
				if err != nil || optionErr != nil {
					t.Fatal(err, optionErr)
				}
				tr := &upstream.Transport{Conn: markedPacketConn{socket}}
				defer tr.Close()
				trace := &ecnFeedbackTrace{counts: make(chan [3]uint64, 16)}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				c, err := tr.Dial(ctx, listener.Addr(), ct, &upstream.Config{
					Versions: []upstream.Version{upstream.Version(v)}, EnableDatagrams: true,
					Tracer: func(context.Context, bool, upstream.ConnectionID) qlogwriter.Trace { return trace },
				})
				if err != nil {
					t.Fatal(err)
				}
				defer c.CloseWithError(0, "")
				peer, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err := c.SendDatagram([]byte("marked data")); err != nil {
					t.Fatal(err)
				}
				b, err := peer.ReceiveDatagram(ctx)
				if err != nil || string(b) != "marked data" {
					t.Fatal(string(b), err)
				}
				select {
				case counts := <-trace.counts:
					index := map[int]int{1: 1, 2: 0, 3: 2}[mark]
					for i, n := range counts {
						if (i == index && n == 0) || (i != index && n != 0) {
							t.Fatal("independent peer decoded wrong counts", counts)
						}
					}
				case <-ctx.Done():
					t.Fatal("no independent ECN ACK observation", ctx.Err())
				}
			})
		}
	}
}
