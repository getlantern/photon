package interop

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
)

type receiveWindowTrace struct {
	params chan qlog.ParametersSet
	phase  atomic.Uint64
}

func (t *receiveWindowTrace) AddProducer() qlogwriter.Recorder { return t }
func (t *receiveWindowTrace) SupportsSchemas(string) bool      { return true }
func (t *receiveWindowTrace) Close() error                     { return nil }
func (t *receiveWindowTrace) RecordEvent(event qlogwriter.Event) {
	if p, ok := event.(qlog.ParametersSet); ok && p.Initiator == qlog.InitiatorRemote {
		select {
		case t.params <- p:
		default:
		}
	}
	if p, ok := event.(qlog.KeyUpdated); ok {
		t.phase.Store(uint64(p.KeyPhase))
	}
}

func TestReceiveWindowsUpstream(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, serverRole := range []bool{false, true} {
			for _, large := range []bool{false, true} {
				for _, direction := range []string{"remote-bidi", "local-bidi", "remote-uni"} {
					t.Run(fmt.Sprintf("%x/server=%t/large=%t/%s", version, serverRole, large, direction), func(t *testing.T) {
						ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
						defer cancel()
						ct, st := tlsConfigs(t)
						cfg := &quic.Config{Versions: []quic.Version{version}, InitialStreamReceiveWindow: 16 << 10, MaxStreamReceiveWindow: 16 << 10, InitialConnectionReceiveWindow: 64 << 10, MaxConnectionReceiveWindow: 64 << 10}
						if large {
							cfg.InitialStreamReceiveWindow = 128 << 10
							cfg.InitialConnectionReceiveWindow = 96 << 10
							cfg.MaxStreamReceiveWindow = 256 << 10
							cfg.MaxConnectionReceiveWindow = 256 << 10
						}
						trace := &receiveWindowTrace{params: make(chan qlog.ParametersSet, 1)}
						pcfg := &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, Tracer: func(context.Context, bool, upstream.ConnectionID) qlogwriter.Trace { return trace }}
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
						select {
						case p := <-trace.params:
							if uint64(p.InitialMaxData) != cfg.InitialConnectionReceiveWindow || uint64(p.InitialMaxStreamDataBidiLocal) != cfg.InitialStreamReceiveWindow || uint64(p.InitialMaxStreamDataBidiRemote) != cfg.InitialStreamReceiveWindow || uint64(p.InitialMaxStreamDataUni) != cfg.InitialStreamReceiveWindow {
								t.Fatal("incorrect advertised receive windows", p)
							}
						case <-ctx.Done():
							t.Fatal("missing peer transport parameters", ctx.Err())
						}
						type sender interface {
							io.Writer
							Close() error
							SetWriteDeadline(time.Time) error
						}
						type receiver interface {
							io.Reader
							SetReadDeadline(time.Time) error
						}
						var out sender
						var in receiver
						if direction == "local-bidi" {
							s, e := own.OpenStreamSync(ctx)
							if e != nil {
								t.Fatal(e)
							}
							if _, e = s.Write([]byte{1}); e != nil {
								t.Fatal(e)
							}
							if e = s.Close(); e != nil {
								t.Fatal(e)
							}
							p, e := peer.AcceptStream(ctx)
							if e != nil {
								t.Fatal(e)
							}
							out, in = p, s
						} else if direction == "remote-uni" {
							out, err = peer.OpenUniStreamSync(ctx)
						} else {
							out, err = peer.OpenStreamSync(ctx)
						}
						if err != nil {
							t.Fatal(err)
						}
						out.SetWriteDeadline(time.Now().Add(5 * time.Second))
						payload := bytes.Repeat([]byte("configured receive window\n"), 11000)
						done := make(chan error, 1)
						go func() {
							_, e := out.Write(payload)
							if e == nil {
								e = out.Close()
							}
							done <- e
						}()
						if in == nil {
							if direction == "remote-uni" {
								in, err = own.AcceptUniStream(ctx)
							} else {
								in, err = own.AcceptStream(ctx)
							}
							if err != nil {
								t.Fatal(err)
							}
						}
						in.SetReadDeadline(time.Now().Add(5 * time.Second))
						data, err := io.ReadAll(in)
						if err != nil || !bytes.Equal(data, payload) {
							t.Fatal("receive window transfer", len(data), err, "peer key phase", trace.phase.Load())
						}
						select {
						case err := <-done:
							if err != nil {
								t.Fatal(err)
							}
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
					})
				}
			}
		}
	}
}

func TestDefaultReceiveWindowsUpstream(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, serverRole := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/server=%t", version, serverRole), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				ct, st := tlsConfigs(t)
				trace := &receiveWindowTrace{params: make(chan qlog.ParametersSet, 1)}
				peerConfig := &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, Tracer: func(context.Context, bool, upstream.ConnectionID) qlogwriter.Trace { return trace }}
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
					own, err := l.Accept(ctx)
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
					own, err := quic.DialAddr(ctx, l.Addr().String(), ct, ownConfig)
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
				select {
				case p := <-trace.params:
					if uint64(p.InitialMaxData) != 768<<10 || uint64(p.InitialMaxStreamDataBidiLocal) != 512<<10 || uint64(p.InitialMaxStreamDataBidiRemote) != 512<<10 || uint64(p.InitialMaxStreamDataUni) != 512<<10 {
						t.Fatalf("default credit observed by independent peer: %+v", p)
					}
				case <-ctx.Done():
					t.Fatal("missing peer transport parameters", ctx.Err())
				}
			})
		}
	}
}
