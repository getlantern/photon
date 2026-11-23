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

func TestDialContextValues(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		for _, shared := range []bool{false, true} {
			t.Run(fmt.Sprintf("%v/shared=%v", version, shared), func(t *testing.T) {
				ct, st := dialTLS(t)
				l, err := ListenAddr("127.0.0.1:0", st, &Config{Versions: []Version{version}})
				if err != nil {
					t.Fatal(err)
				}
				defer l.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				dialCtx, stopDial := context.WithCancel(context.WithValue(ctx, transportContextKey{}, "dial metadata"))
				defer stopDial()
				var client *Conn
				if shared {
					socket, e := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
					if e != nil {
						t.Fatal(e)
					}
					defer socket.Close()
					tr := &Transport{Conn: socket}
					defer tr.Close()
					client, err = tr.Dial(dialCtx, l.Addr(), ct, nil)
				} else {
					client, err = DialAddr(dialCtx, l.Addr().String(), ct, nil)
				}
				if err != nil {
					t.Fatal(err)
				}
				defer client.server.Close()
				server, err := l.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				stopDial()
				if client.Context().Value(transportContextKey{}) != "dial metadata" || client.Context().Err() != nil {
					t.Fatal("lost dial metadata or retained cancellation", client.Context())
				}
				if _, ok := client.Context().Deadline(); ok {
					t.Fatal("retained handshake deadline")
				}
				stream, err := client.OpenStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				for _, lifetime := range []context.Context{stream.Context(), stream.ReceiveContext()} {
					if lifetime.Value(transportContextKey{}) != "dial metadata" || lifetime.Err() != nil {
						t.Fatal("lost stream metadata or retained cancellation")
					}
				}
				if _, err := stream.Write([]byte("survives dial cancellation")); err != nil {
					t.Fatal(err)
				}
				peer, err := server.AcceptStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				peer.SetReadDeadline(time.Now().Add(time.Second))
				payload := make([]byte, len("survives dial cancellation"))
				if _, err := io.ReadFull(peer, payload); err != nil || string(payload) != "survives dial cancellation" {
					t.Fatal(string(payload), err)
				}
				if err := server.CloseWithError(45, "peer close"); err != nil {
					t.Fatal(err)
				}
				for _, lifetime := range []context.Context{client.Context(), stream.Context(), stream.ReceiveContext()} {
					select {
					case <-lifetime.Done():
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					var app *ApplicationError
					if !errors.As(context.Cause(lifetime), &app) || app.ErrorCode != 45 || !app.Remote {
						t.Fatal(context.Cause(lifetime))
					}
				}
			})
		}
	}
}
