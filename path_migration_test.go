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

func TestPublicAdditionalPathMigration(t *testing.T) {
	for _, grease := range []bool{false, true} {
		t.Run(fmt.Sprintf("grease=%v", grease), func(t *testing.T) { testPublicAdditionalPathMigration(t, grease) })
	}
}
func testPublicAdditionalPathMigration(t *testing.T, grease bool) {
	for _, version := range []Version{Version1, Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ct, st := dialTLS(t)
			listener, err := ListenAddr("127.0.0.1:0", st, &Config{EnableMultipath: true, EnableQUICBitGreasing: grease})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			bind := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}
			client, err := DialAddr(ctx, listener.Addr().String(), ct, &Config{Versions: []Version{version}, EnableMultipath: true, EnableQUICBitGreasing: grease, LocalAddr: bind})
			if err != nil {
				t.Fatal(err)
			}
			defer client.server.Close()
			server, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			primary := client.LocalAddr().String()
			path, err := client.OpenPath(ctx, 1, bind, listener.Addr().(*net.UDPAddr))
			if err != nil {
				t.Fatal(err)
			}
			old := path.LocalAddr().String()
			canceled, stop := context.WithCancel(ctx)
			stop()
			if err := path.Migrate(canceled, bind); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if path.LocalAddr().String() != old {
				t.Fatal("canceled path migration changed address")
			}
			if err := path.Migrate(ctx, bind); err != nil {
				t.Fatal(err)
			}
			migrated := path.LocalAddr().(*net.UDPAddr)
			if migrated.String() == old || path.ID() != 1 || client.LocalAddr().String() != primary {
				t.Fatal("path migration changed wrong identity")
			}
			if err := client.AbandonPath(ctx, 0); err != nil {
				t.Fatal(err)
			}
			exchange := func(message string) {
				t.Helper()
				send, err := client.OpenStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				send.SetDeadline(time.Now().Add(3 * time.Second))
				if _, err := send.Write([]byte(message)); err != nil {
					t.Fatal(err)
				}
				if err := send.Close(); err != nil {
					t.Fatal(err)
				}
				receive, err := server.AcceptStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				receive.SetDeadline(time.Now().Add(3 * time.Second))
				got, err := io.ReadAll(receive)
				if err != nil || string(got) != message {
					t.Fatalf("request %q: %v", got, err)
				}
				if _, err := receive.Write(got); err != nil {
					t.Fatal(err)
				}
				if err := receive.Close(); err != nil {
					t.Fatal(err)
				}
				got, err = io.ReadAll(send)
				if err != nil || string(got) != message {
					t.Fatalf("reply %q: %v", got, err)
				}
			}
			exchange("only migrated path remains")
			_, observed, ok := server.PathAddresses(1)
			if !ok || observed.String() != migrated.String() {
				t.Fatal("server did not observe migrated path")
			}
			second, err := client.OpenPath(ctx, 2, bind, listener.Addr().(*net.UDPAddr))
			if err != nil {
				t.Fatal(err)
			}
			defer second.Close()
			if err := path.Close(); err != nil {
				t.Fatal(err)
			}
			if err := path.Close(); err != nil {
				t.Fatal("repeated close", err)
			}
			released, err := net.ListenUDP("udp4", migrated)
			if err != nil {
				t.Fatal("migrated socket still open", err)
			}
			released.Close()
			if err := path.Migrate(ctx, bind); err == nil {
				t.Fatal("abandoned path migrated")
			}
			exchange("survives migrated path close")
		})
	}
}
