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

type connectionBlockedObservation struct {
	limit uint64
}
type connectionDataBlockedTrace struct {
	frames chan connectionBlockedObservation
}

func (t *connectionDataBlockedTrace) AddProducer() qlogwriter.Recorder { return t }
func (t *connectionDataBlockedTrace) SupportsSchemas(string) bool      { return true }
func (t *connectionDataBlockedTrace) Close() error                     { return nil }
func (t *connectionDataBlockedTrace) RecordEvent(event qlogwriter.Event) {
	if p, ok := event.(qlog.PacketReceived); ok {
		for _, frame := range p.Frames {
			if f, ok := frame.Frame.(*qlog.DataBlockedFrame); ok {
				select {
				case t.frames <- connectionBlockedObservation{limit: uint64(f.MaximumData)}:
				default:
				}
			}
		}
	}
}

func TestDataBlockedUpstream(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, serverRole := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/server=%t", version, serverRole), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				ct, st := tlsConfigs(t)
				trace := &connectionDataBlockedTrace{frames: make(chan connectionBlockedObservation, 8)}
				var own *quic.Conn
				peerConfig := &upstream.Config{InitialConnectionReceiveWindow: 1, MaxConnectionReceiveWindow: 1, InitialStreamReceiveWindow: 16, MaxStreamReceiveWindow: 16, Versions: []upstream.Version{upstream.Version(version)}, Tracer: func(context.Context, bool, upstream.ConnectionID) qlogwriter.Trace { return trace }}
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

				tx, err := own.OpenUniStream()
				if err != nil {
					t.Fatal(err)
				}
				if err := tx.SetWriteDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
					t.Fatal(err)
				}
				n, err := tx.Write([]byte{1, 2})
				if n != 1 || !errors.Is(err, os.ErrDeadlineExceeded) {
					t.Fatal("connection-blocked write", n, err)
				}
				select {
				case f := <-trace.frames:
					if f.limit != 1 {
						t.Fatal("wrong connection-blocked limit", f)
					}
				case <-ctx.Done():
					t.Fatal("missing independent DATA_BLOCKED", ctx.Err())
				}
			})
		}
	}
}
