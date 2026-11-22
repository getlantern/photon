package quic

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"
)

func TestPublicQUICBitGreasing(t *testing.T) {
	for _, v := range []Version{Version1, Version2} {
		for _, clientSupport := range []bool{false, true} {
			for _, serverSupport := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/client=%v/server=%v", v, clientSupport, serverSupport), func(t *testing.T) {
					ct, st := dialTLS(t)
					listener, err := ListenAddr("127.0.0.1:0", st, &Config{Versions: []Version{v}, EnableQUICBitGreasing: serverSupport})
					if err != nil {
						t.Fatal(err)
					}
					defer listener.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					client, err := DialAddr(ctx, listener.Addr().String(), ct, &Config{Versions: []Version{v}, EnableQUICBitGreasing: clientSupport})
					if err != nil {
						t.Fatal(err)
					}
					defer client.CloseWithError(0, "")
					server, err := listener.Accept(ctx)
					if err != nil {
						t.Fatal(err)
					}
					stream, err := client.OpenStreamSync(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := stream.Write([]byte("greased request")); err != nil {
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
					if err != nil || string(payload) != "greased request" {
						t.Fatal(string(payload), err)
					}
					if _, err := incoming.Write([]byte("greased response")); err != nil {
						t.Fatal(err)
					}
					if err := incoming.Close(); err != nil {
						t.Fatal(err)
					}
					payload, err = io.ReadAll(stream)
					if err != nil || string(payload) != "greased response" {
						t.Fatal(string(payload), err)
					}
				})
			}
		}
	}
}
