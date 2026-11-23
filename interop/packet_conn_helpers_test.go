package interop

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

func TestPacketConnHelpersIndependent(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, early := range []bool{false, true} {
			for _, ownServer := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/early=%v/server=%v", version, early, ownServer), func(t *testing.T) {
					ct, st := tlsConfigs(t)
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
					if err != nil {
						t.Fatal(err)
					}
					defer socket.Close()
					cfg := &quic.Config{Versions: []quic.Version{version}}
					peerCfg := &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}
					var own *quic.Conn
					var peer *upstream.Conn
					if ownServer {
						var l *quic.Listener
						if early {
							el, e := quic.ListenEarly(socket, st, cfg)
							err = e
							if el != nil {
								l = el.Listener
							}
						} else {
							l, err = quic.Listen(socket, st, cfg)
						}
						if err != nil {
							t.Fatal(err)
						}
						defer l.Close()
						peer, err = dialUpstreamAddr(t, ctx, l.Addr().String(), ct, peerCfg)
						if err != nil {
							t.Fatal(err)
						}
						defer peer.CloseWithError(0, "")
						own, err = l.Accept(ctx)
					} else {
						l, e := upstream.ListenAddr("127.0.0.1:0", st, peerCfg)
						if e != nil {
							t.Fatal(e)
						}
						defer l.Close()
						if early {
							own, err = quic.DialEarly(ctx, socket, l.Addr(), ct, cfg)
						} else {
							own, err = quic.Dial(ctx, socket, l.Addr(), ct, cfg)
						}
						if err != nil {
							t.Fatal(err)
						}
						defer own.CloseWithError(0, "")
						peer, err = l.Accept(ctx)
						if err == nil {
							defer peer.CloseWithError(0, "")
						}
					}
					if err != nil {
						t.Fatal(err)
					}
					defer own.CloseWithError(0, "")
					payload := bytes.Repeat([]byte("independent helper"), 1000)
					tx, err := own.OpenUniStreamSync(ctx)
					if err != nil {
						t.Fatal(err)
					}
					tx.SetWriteDeadline(time.Now().Add(time.Second))
					if _, err = tx.Write(payload); err != nil {
						t.Fatal(err)
					}
					if err = tx.Close(); err != nil {
						t.Fatal(err)
					}
					rx, err := peer.AcceptUniStream(ctx)
					if err != nil {
						t.Fatal(err)
					}
					rx.SetReadDeadline(time.Now().Add(time.Second))
					got, err := io.ReadAll(rx)
					if err != nil || !bytes.Equal(got, payload) {
						t.Fatal("own to peer", len(got), err)
					}
					out, err := peer.OpenUniStreamSync(ctx)
					if err != nil {
						t.Fatal(err)
					}
					out.SetWriteDeadline(time.Now().Add(time.Second))
					if _, err = out.Write(payload); err != nil {
						t.Fatal(err)
					}
					if err = out.Close(); err != nil {
						t.Fatal(err)
					}
					in, err := own.AcceptUniStream(ctx)
					if err != nil {
						t.Fatal(err)
					}
					in.SetReadDeadline(time.Now().Add(time.Second))
					got, err = io.ReadAll(in)
					if err != nil || !bytes.Equal(got, payload) {
						t.Fatal("peer to own", len(got), err)
					}
				})
			}
		}
	}
}
