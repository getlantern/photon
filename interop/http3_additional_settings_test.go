package interop

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

func TestHTTP3AdditionalSettingsUpstream(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, mode := range []string{"client-direct", "client-pooled", "server"} {
			t.Run(fmt.Sprintf("%x/%s", v, mode), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				ct.NextProtos = []string{"h3"}
				st.NextProtos = []string{"h3"}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				extra := map[uint64]uint64{0xface: 77, 0x21: 0, (1 << 62) - 1: (1 << 62) - 1}
				check := func(settings *peerh3.Settings) error {
					if settings == nil {
						return fmt.Errorf("settings absent")
					}
					for id, value := range extra {
						if got, ok := settings.Other[id]; !ok || got != value {
							return fmt.Errorf("setting %x = %d present=%v", id, got, ok)
						}
					}
					if _, ok := settings.Other[6]; ok {
						return fmt.Errorf("max field size exposed as extra")
					}
					return nil
				}
				if mode == "server" {
					listener, err := quic.ListenAddr("127.0.0.1:0", st, &quic.Config{Versions: []quic.Version{v}})
					if err != nil {
						t.Fatal(err)
					}
					server := &ownh3.Server{AdditionalSettings: extra, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") })}
					served := make(chan error, 1)
					go func() { served <- server.Serve(ctx, listener) }()
					defer func() { server.Close(); listener.Close(); <-served }()
					peer, err := dialUpstreamAddr(t, ctx, listener.Addr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}})
					if err != nil {
						t.Fatal(err)
					}
					defer peer.CloseWithError(0, "done")
					transport := &peerh3.Transport{Dial: loopbackDial(t)}
					defer transport.Close()
					client := transport.NewClientConn(peer)
					select {
					case <-client.ReceivedSettings():
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					if err := check(client.Settings()); err != nil {
						t.Fatal(err)
					}
					req, _ := http.NewRequestWithContext(ctx, "GET", "https://"+listener.Addr().String(), nil)
					response, err := client.RoundTrip(req)
					if err != nil {
						t.Fatal(err)
					}
					defer response.Body.Close()
					body, err := io.ReadAll(response.Body)
					if err != nil || string(body) != "ok" {
						t.Fatal(string(body), err)
					}
					return
				}
				inspected := make(chan error, 1)
				server := &peerh3.Server{TLSConfig: st, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					settings := w.(peerh3.Settingser)
					select {
					case <-settings.ReceivedSettings():
					case <-r.Context().Done():
						inspected <- r.Context().Err()
						return
					}
					inspected <- check(settings.Settings())
					io.WriteString(w, "ok")
				})}
				socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				served := make(chan error, 1)
				go func() { served <- server.Serve(socket) }()
				defer func() { server.Close(); socket.Close(); <-served }()
				origin := "https://" + socket.LocalAddr().String()
				var client http.RoundTripper
				if mode == "client-direct" {
					c, err := ownh3.Dial(ctx, origin, ct, &quic.Config{Versions: []quic.Version{v}}, &ownh3.ClientConfig{AdditionalSettings: extra})
					if err != nil {
						t.Fatal(err)
					}
					defer c.Close()
					client = c
				} else {
					c := &ownh3.Transport{TLSClientConfig: ct, QUICConfig: &quic.Config{Versions: []quic.Version{v}}, AdditionalSettings: map[uint64]uint64{0xface: 77}, ClientConfig: &ownh3.ClientConfig{AdditionalSettings: map[uint64]uint64{0x21: 0, (1 << 62) - 1: (1 << 62) - 1}}}
					defer c.Close()
					client = c
				}
				req, _ := http.NewRequestWithContext(ctx, "GET", origin, nil)
				response, err := client.RoundTrip(req)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				body, err := io.ReadAll(response.Body)
				if err != nil || string(body) != "ok" {
					t.Fatal(string(body), err)
				}
				select {
				case err := <-inspected:
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
