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

func TestPublicLocalMigration(t *testing.T) {
	for _, grease := range []bool{false, true} {
		t.Run(fmt.Sprintf("grease=%v", grease), func(t *testing.T) { testPublicLocalMigration(t, grease) })
	}
}
func testPublicLocalMigration(t *testing.T, grease bool) {
	for _, version := range []Version{Version1, Version2} {
		for _, mp := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/multipath=%v", version, mp), func(t *testing.T) {
				ct, st := dialTLS(t)
				config := &Config{Versions: []Version{version}, EnableMultipath: mp, EnableQUICBitGreasing: grease}
				listener, err := ListenAddr("127.0.0.1:0", st, config)
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				clientConfig := *config
				clientConfig.LocalAddr = &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}
				client, err := DialAddr(ctx, listener.Addr().String(), ct, &clientConfig)
				if err != nil {
					t.Fatal(err)
				}
				defer client.CloseWithError(0, "")
				server, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				old := client.LocalAddr().String()
				canceled, cancelNow := context.WithCancel(ctx)
				cancelNow()
				if err := client.Migrate(canceled, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}); !errors.Is(err, context.Canceled) {
					t.Fatal("canceled migration", err)
				}
				if client.LocalAddr().String() != old {
					t.Fatal("canceled migration changed address")
				}
				if err := server.Migrate(ctx, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}); err == nil {
					t.Fatal("server initiated migration")
				}
				for range 2 {
					if err := client.Migrate(ctx, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}); err != nil {
						t.Fatal(err)
					}
					if client.LocalAddr().String() == old {
						t.Fatal("migration retained old address")
					}
					old = client.LocalAddr().String()
					stream, err := client.OpenStreamSync(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := stream.Write([]byte("migrated")); err != nil {
						t.Fatal(err)
					}
					if err := stream.Close(); err != nil {
						t.Fatal(err)
					}
					received, err := server.AcceptStream(ctx)
					if err != nil {
						t.Fatal(err)
					}
					data, err := io.ReadAll(received)
					if err != nil || string(data) != "migrated" {
						t.Fatal("migration stream", string(data), err)
					}
					if server.RemoteAddr().String() != old {
						t.Fatal("server retained old peer address")
					}
					if _, err := received.Write([]byte("reply")); err != nil {
						t.Fatal(err)
					}
					if err := received.Close(); err != nil {
						t.Fatal(err)
					}
					data, err = io.ReadAll(stream)
					if err != nil || string(data) != "reply" {
						t.Fatal("return stream", string(data), err)
					}
				}
			})
		}
	}
}
