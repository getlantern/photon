package interop

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
)

type initialSizeTrace struct {
	initial chan int
	maximum atomic.Int64
}

func (t *initialSizeTrace) AddProducer() qlogwriter.Recorder { return t }
func (t *initialSizeTrace) SupportsSchemas(string) bool      { return true }
func (t *initialSizeTrace) Close() error                     { return nil }
func (t *initialSizeTrace) RecordEvent(event qlogwriter.Event) {
	if p, ok := event.(qlog.PacketReceived); ok {
		for {
			old := t.maximum.Load()
			if int64(p.Raw.Length) <= old || t.maximum.CompareAndSwap(old, int64(p.Raw.Length)) {
				break
			}
		}
		if p.Header.PacketType == qlog.PacketTypeInitial {
			select {
			case t.initial <- p.Raw.Length:
			default:
			}
		}
	}
}

func TestInitialPacketSizeUpstream(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, serverRole := range []bool{false, true} {
			for _, size := range []uint16{0, 1200, 1452} {
				t.Run(fmt.Sprintf("%x/server=%t/size=%d", v, serverRole, size), func(t *testing.T) {
					ct, st := tlsConfigs(t)
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					cfg := &quic.Config{Versions: []quic.Version{v}, InitialPacketSize: size, DisablePathMTUDiscovery: true, EnableDatagrams: true}
					trace := &initialSizeTrace{initial: make(chan int, 1)}
					pcfg := &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}, EnableDatagrams: true, Tracer: func(context.Context, bool, upstream.ConnectionID) qlogwriter.Trace { return trace }}
					var own *quic.Conn
					var peer *upstream.Conn
					var err error
					if serverRole {
						listener, e := quic.ListenAddr("127.0.0.1:0", st, cfg)
						if e != nil {
							t.Fatal(e)
						}
						defer listener.Close()
						peer, err = dialUpstreamAddr(t, ctx, listener.Addr().String(), ct, pcfg)
						if err != nil {
							t.Fatal(err)
						}
						defer peer.CloseWithError(0, "")
						own, err = listener.Accept(ctx)
					} else {
						listener, e := upstream.ListenAddr("127.0.0.1:0", st, pcfg)
						if e != nil {
							t.Fatal(e)
						}
						defer listener.Close()
						own, err = quic.DialAddr(ctx, listener.Addr().String(), ct, cfg)
						if err != nil {
							t.Fatal(err)
						}
						defer own.CloseWithError(0, "")
						peer, err = listener.Accept(ctx)
					}
					if err != nil {
						t.Fatal(err)
					}
					defer own.CloseWithError(0, "")
					defer peer.CloseWithError(0, "")
					want := int(size)
					if want == 0 {
						want = 1280
					}
					select {
					case got := <-trace.initial:
						if got != want {
							t.Fatal("peer observed Initial size", got, want)
						}
					case <-ctx.Done():
						t.Fatal("no peer Initial observation", ctx.Err())
					}
					payload := bytes.Repeat([]byte{73}, want-42)
					oversize := own.SendDatagram(make([]byte, len(payload)+1))
					var sizeError *quic.DatagramTooLargeError
					if !errors.As(oversize, &sizeError) || sizeError.MaxDatagramPayloadSize != int64(len(payload)) || !errors.Is(oversize, &quic.DatagramTooLargeError{MaxDatagramPayloadSize: int64(len(payload))}) {
						t.Fatalf("DATAGRAM admission limit: %T %v", oversize, oversize)
					}
					if err := own.SendDatagram(payload); err != nil {
						t.Fatal(err)
					}
					got, err := peer.ReceiveDatagram(ctx)
					if err != nil || !bytes.Equal(got, payload) {
						t.Fatal("configured DATAGRAM delivery", len(got), err)
					}
					if max := trace.maximum.Load(); max > int64(want) {
						t.Fatal("peer saw packet above configured limit", max, want)
					}
				})
			}
		}
	}
}
