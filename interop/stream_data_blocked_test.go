package interop

import (
	"context"
	"errors"
	"fmt"
	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
	"os"
	"testing"
	"time"
)

type dataBlockedObservation struct {
	id    uint64
	limit uint64
}
type streamDataBlockedTrace struct{ frames chan dataBlockedObservation }

func (t *streamDataBlockedTrace) AddProducer() qlogwriter.Recorder { return t }
func (t *streamDataBlockedTrace) SupportsSchemas(string) bool      { return true }
func (t *streamDataBlockedTrace) Close() error                     { return nil }
func (t *streamDataBlockedTrace) RecordEvent(event qlogwriter.Event) {
	if p, ok := event.(qlog.PacketReceived); ok {
		for _, frame := range p.Frames {
			if f, ok := frame.Frame.(*qlog.StreamDataBlockedFrame); ok {
				select {
				case t.frames <- dataBlockedObservation{id: uint64(f.StreamID), limit: uint64(f.MaximumStreamData)}:
				default:
				}
			}
		}
	}
}

func TestStreamDataBlockedUpstream(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, serverRole := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/server=%t", version, serverRole), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				ct, st := tlsConfigs(t)
				trace := &streamDataBlockedTrace{frames: make(chan dataBlockedObservation, 8)}
				var own *quic.Conn
				peerConfig := &upstream.Config{InitialStreamReceiveWindow: 1, MaxStreamReceiveWindow: 1, Versions: []upstream.Version{upstream.Version(version)}, Tracer: func(context.Context, bool, upstream.ConnectionID) qlogwriter.Trace { return trace }}
				ownConfig := &quic.Config{Versions: []quic.Version{version}}
				if serverRole {
					l, err := quic.ListenAddr("127.0.0.1:0", st, ownConfig)
					if err != nil {
						t.Fatal(err)
					}
					defer l.Close()
					peer, err := dialUpstreamAddr(t, ctx, l.Addr().String(), ct, peerConfig)
					if err != nil {
						t.Fatal(err)
					}
					defer peer.CloseWithError(0, "")
					own, err = l.Accept(ctx)
					if err != nil {
						t.Fatal(err)
					}
					defer own.CloseWithError(0, "")
				} else {
					l, err := upstream.ListenAddr("127.0.0.1:0", st, peerConfig)
					if err != nil {
						t.Fatal(err)
					}
					defer l.Close()
					own, err = quic.DialAddr(ctx, l.Addr().String(), ct, ownConfig)
					if err != nil {
						t.Fatal(err)
					}
					defer own.CloseWithError(0, "")
					peer, err := l.Accept(ctx)
					if err != nil {
						t.Fatal(err)
					}
					defer peer.CloseWithError(0, "")
				}

				for _, uni := range []bool{false, true} {
					var tx interface {
						Write([]byte) (int, error)
						SetWriteDeadline(time.Time) error
						StreamID() quic.StreamID
					}
					var err error
					if uni {
						tx, err = own.OpenUniStream()
					} else {
						tx, err = own.OpenStream()
					}
					if err != nil {
						t.Fatal(err)
					}
					if err := tx.SetWriteDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
						t.Fatal(err)
					}
					n, err := tx.Write([]byte{1, 2})
					if n != 1 || !errors.Is(err, os.ErrDeadlineExceeded) {
						t.Fatal("blocked public write", n, err)
					}
					select {
					case f := <-trace.frames:
						if f.id != uint64(tx.StreamID()) || f.limit != 1 {
							t.Fatal("wrong blocked signal", f)
						}
					case <-ctx.Done():
						t.Fatal("missing independent blocked signal", ctx.Err())
					}
				}
			})
		}
	}
}
