package quic

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func TestPublicCompatibleVersionNegotiation(t *testing.T) {
	for _, v := range []Version{Version1, Version2} {
		selected := Version1
		if selected == v {
			selected = Version2
		}
		for _, host := range []string{"127.0.0.1", "::1"} {
			for _, clientSupport := range []bool{false, true} {
				for _, serverSupport := range []bool{false, true} {
					t.Run(fmt.Sprintf("%x/%s/client=%v/server=%v", v, host, clientSupport, serverSupport), func(t *testing.T) {
						ct, st := dialTLS(t)
						ct.ServerName = "127.0.0.1"
						listener, err := ListenAddr(net.JoinHostPort(host, "0"), st, &Config{Versions: []Version{selected, v}, EnableCompatibleVersionNegotiation: serverSupport})
						if err != nil {
							t.Fatal(err)
						}
						defer listener.Close()
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						client, err := DialAddr(ctx, listener.Addr().String(), ct, &Config{InitialVersion: v, Versions: []Version{selected, v}, EnableCompatibleVersionNegotiation: clientSupport})
						if err != nil {
							t.Fatal(err)
						}
						defer client.CloseWithError(0, "")
						server, err := listener.Accept(ctx)
						if err != nil {
							t.Fatal(err)
						}
						want := v
						if clientSupport && serverSupport {
							want = selected
						}
						if client.ConnectionState().Version != want || server.ConnectionState().Version != want {
							t.Fatalf("versions client=%x server=%x want=%x", client.ConnectionState().Version, server.ConnectionState().Version, want)
						}
						stream, err := client.OpenStreamSync(ctx)
						if err != nil {
							t.Fatal(err)
						}
						if _, err := stream.Write([]byte("versioned request")); err != nil {
							t.Fatal(err)
						}
						if err := stream.Close(); err != nil {
							t.Fatal(err)
						}
						incoming, err := server.AcceptStream(ctx)
						if err != nil {
							t.Fatal(err)
						}
						payload, err := io.ReadAll(incoming)
						if err != nil || string(payload) != "versioned request" {
							t.Fatal(string(payload), err)
						}
						if _, err := incoming.Write([]byte("versioned response")); err != nil {
							t.Fatal(err)
						}
						if err := incoming.Close(); err != nil {
							t.Fatal(err)
						}
						payload, err = io.ReadAll(stream)
						if err != nil || string(payload) != "versioned response" {
							t.Fatal(string(payload), err)
						}
					})
				}
			}
		}
	}

}

func TestInitialVersionConfiguration(t *testing.T) {
	for _, cfg := range []Config{{InitialVersion: 99}, {Versions: []Version{Version1}, InitialVersion: Version2}} {
		if _, _, err := prepareConfig(&cfg); err == nil {
			t.Fatal("initial version outside supported list accepted")
		}
	}
	for _, cfg := range []Config{{InitialVersion: Version2}, {Versions: []Version{Version2, Version1}, InitialVersion: Version1}} {
		if _, _, err := prepareConfig(&cfg); err != nil {
			t.Fatal(err)
		}
	}
}
