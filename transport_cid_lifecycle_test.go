package quic

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func TestTransportCIDGeneratorMigrationAndRestart(t *testing.T) {
	for _, version := range []Version{Version1, Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ct, st := dialTLS(t)
			transports := make([]*Transport, 2)
			bind := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}
			for i := range transports {
				socket, err := net.ListenUDP("udp4", bind)
				if err != nil {
					t.Fatal(err)
				}
				defer socket.Close()
				transports[i] = &Transport{Conn: socket, ConnectionIDGenerator: &testCIDGenerator{length: 8 + i*4}}
				defer transports[i].Close()
			}
			cfg := &Config{Versions: []Version{version}, EnableMultipath: true}
			listener, err := transports[0].Listen(st, cfg)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			client, err := transports[1].Dial(ctx, listener.Addr(), ct, cfg)
			if err != nil {
				t.Fatal(err)
			}
			server, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			path, err := client.OpenPath(ctx, 1, bind, listener.Addr().(*net.UDPAddr))
			if err != nil {
				t.Fatal(err)
			}
			if err := path.Migrate(ctx, bind); err != nil {
				t.Fatal(err)
			}
			if err := client.AbandonPath(ctx, 0); err != nil {
				t.Fatal(err)
			}
			if err := listener.Close(); err != nil {
				t.Fatal(err)
			}
			listener, err = transports[0].Listen(st, cfg)
			if err != nil {
				t.Fatal(err)
			}
			next, err := transports[1].Dial(ctx, listener.Addr(), ct, cfg)
			if err != nil {
				t.Fatal(err)
			}
			nextServer, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for i, pair := range [][2]*Conn{{client, server}, {next, nextServer}} {
				stream, err := pair[0].OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				payload := fmt.Sprintf("custom CID generation %d", i)
				if _, err := stream.Write([]byte(payload)); err != nil {
					t.Fatal(err)
				}
				stream.Close()
				receive, err := pair[1].AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				receive.SetReadDeadline(time.Now().Add(time.Second))
				got, err := io.ReadAll(receive)
				if err != nil || string(got) != payload {
					t.Fatal(string(got), err)
				}
			}
		})
	}
}
