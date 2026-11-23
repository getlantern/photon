package quic

import (
	"context"
	"errors"
	"fmt"
	"github.com/getlantern/photon/internal/transport"
	"net"
	"testing"
	"time"
)

func TestTransportStatelessResetRestart(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ct, st := dialTLS(t)
			socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer socket.Close()
			key := StatelessResetKey{7}
			tr := &Transport{Conn: socket, StatelessResetKey: &key}
			defer func() { tr.Close() }()
			l, err := tr.Listen(st, &Config{Versions: []Version{version}})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := DialAddr(ctx, l.Addr().String(), ct, &Config{Versions: []Version{version}})
			if err != nil {
				t.Fatal(err)
			}
			defer client.server.Close()
			if _, err := l.Accept(ctx); err != nil {
				t.Fatal(err)
			}
			stream, err := client.OpenUniStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := tr.Close(); err != nil {
				t.Fatal(err)
			}
			tr = &Transport{Conn: socket, StatelessResetKey: &key}
			if _, err := tr.Listen(st, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := stream.Write(make([]byte, 1000)); err != nil {
				t.Fatal(err)
			}
			select {
			case <-client.Context().Done():
			case <-ctx.Done():
				t.Fatal("peer did not detect reset", ctx.Err())
			}
			var reset *StatelessResetError
			if !errors.As(context.Cause(client.Context()), &reset) {
				t.Fatal("wrong termination", context.Cause(client.Context()))
			}
		})
	}
}

func TestTransportDialedStatelessResetRestart(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ct, st := dialTLS(t)
			listener, err := ListenAddr("127.0.0.1:0", st, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer socket.Close()
			key := StatelessResetKey{8}
			tr := &Transport{Conn: socket, StatelessResetKey: &key}
			defer func() { tr.Close() }()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := tr.Dial(ctx, listener.Addr(), ct, &Config{Versions: []Version{version}}); err != nil {
				t.Fatal(err)
			}
			peer, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			// Clients cannot advertise an initial reset parameter. Select a CID issued
			// in NEW_CONNECTION_ID, whose token the server can use after client restart.
			if err := peer.server.Wait(ctx, func(*transport.ServerEndpoint) (bool, error) { return peer.raw.RotatePeerID() == nil, nil }); err != nil {
				t.Fatal(err)
			}
			stream, err := peer.OpenUniStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := tr.Close(); err != nil {
				t.Fatal(err)
			}
			tr = &Transport{Conn: socket, StatelessResetKey: &key}
			if _, err := tr.init(16); err != nil {
				t.Fatal(err)
			}
			if _, err := stream.Write(make([]byte, 1000)); err != nil {
				t.Fatal(err)
			}
			select {
			case <-peer.Context().Done():
			case <-ctx.Done():
				t.Fatal("server did not detect client reset", ctx.Err())
			}
			var reset *StatelessResetError
			if !errors.As(context.Cause(peer.Context()), &reset) {
				t.Fatal(context.Cause(peer.Context()))
			}
		})
	}
}
