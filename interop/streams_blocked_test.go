package interop

import (
	"context"
	"errors"
	"fmt"
	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
	"testing"
	"time"
)

type blockedObservation struct {
	uni   bool
	limit uint64
}
type streamsBlockedTrace struct{ frames chan blockedObservation }

func (t *streamsBlockedTrace) AddProducer() qlogwriter.Recorder { return t }
func (t *streamsBlockedTrace) SupportsSchemas(string) bool      { return true }
func (t *streamsBlockedTrace) Close() error                     { return nil }
func (t *streamsBlockedTrace) RecordEvent(event qlogwriter.Event) {
	if p, ok := event.(qlog.PacketReceived); ok {
		for _, frame := range p.Frames {
			if f, ok := frame.Frame.(*qlog.StreamsBlockedFrame); ok {
				select {
				case t.frames <- blockedObservation{uni: f.Type == 0, limit: uint64(f.StreamLimit)}:
				default:
				}
			}
		}
	}
}

func TestStreamsBlockedUpstream(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, serverRole := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/server=%t", version, serverRole), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				ct, st := tlsConfigs(t)
				trace := &streamsBlockedTrace{frames: make(chan blockedObservation, 8)}
				var own *quic.Conn
				var peer *upstream.Conn
				peerConfig := &upstream.Config{MaxIncomingStreams: -1, MaxIncomingUniStreams: -1, Versions: []upstream.Version{upstream.Version(version)}, Tracer: func(context.Context, bool, upstream.ConnectionID) qlogwriter.Trace { return trace }}
				ownConfig := &quic.Config{MaxIncomingStreams: -1, MaxIncomingUniStreams: -1, Versions: []quic.Version{version}}
				if serverRole {
					l, err := quic.ListenAddr("127.0.0.1:0", st, ownConfig)
					if err != nil {
						t.Fatal(err)
					}
					defer l.Close()
					peer, err = dialUpstreamAddr(t, ctx, l.Addr().String(), ct, peerConfig)
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
					peer, err = l.Accept(ctx)
					if err != nil {
						t.Fatal(err)
					}
					defer peer.CloseWithError(0, "")
				}
				for _, uni := range []bool{false, true} {
					blockedCtx, stop := context.WithCancel(ctx)
					done := make(chan error, 1)
					go func() {
						var err error
						if uni {
							_, err = own.OpenUniStreamSync(blockedCtx)
						} else {
							_, err = own.OpenStreamSync(blockedCtx)
						}
						done <- err
					}()
					select {
					case f := <-trace.frames:
						if f.uni != uni || f.limit != 0 {
							t.Fatal("wrong blocked signal", f)
						}
					case <-ctx.Done():
						stop()
						t.Fatal("missing independent blocked signal", ctx.Err())
					}
					stop()
					select {
					case err := <-done:
						if !errors.Is(err, context.Canceled) {
							t.Fatal("blocked open completed", err)
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					var ownErr, peerErr error
					if uni {
						_, ownErr = own.OpenUniStream()
						_, peerErr = peer.OpenUniStream()
					} else {
						_, ownErr = own.OpenStream()
						_, peerErr = peer.OpenStream()
					}
					var ownLimit *quic.StreamLimitReachedError
					var peerLimit *upstream.StreamLimitReachedError
					if !errors.As(ownErr, &ownLimit) || !errors.As(peerErr, &peerLimit) || !errors.Is(ownErr, &quic.StreamLimitReachedError{}) {
						t.Fatalf("nonblocking limit errors: own %T %v, peer %T %v", ownErr, ownErr, peerErr, peerErr)
					}
					if ownErr.Error() != peerErr.Error() {
						t.Fatalf("limit error text: own %q, peer %q", ownErr, peerErr)
					}

				}
			})
		}
	}
}
