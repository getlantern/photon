package interop

import (
	"context"
	"crypto/tls"
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

func TestHTTP3DialSettingsBoundResumption(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", v), func(t *testing.T) {
			ct, st := tlsConfigs(t)
			cache := &observedSessionCache{ClientSessionCache: tls.NewLRUClientSessionCache(4), stored: make(chan bool, 8)}
			ct.ClientSessionCache = cache
			peer := &peerh3.Server{TLSConfig: st, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("resumed HTTP")) })}
			socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() { defer close(done); peer.Serve(socket) }()
			defer func() { peer.Close(); socket.Close(); <-done }()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			origin := "https://" + socket.LocalAddr().String()
			for round := 0; round < 2; round++ {
				client, err := ownh3.Dial(ctx, origin, ct, &quic.Config{Versions: []quic.Version{v}}, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				request, _ := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/", nil)
				response, err := client.RoundTrip(request)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil || string(body) != "resumed HTTP" {
					t.Fatal(string(body), err)
				}
				if response.TLS == nil || response.TLS.DidResume != (round == 1) {
					t.Fatal("HTTP resumption state", round)
				}
				select {
				case early := <-cache.stored:
					if early {
						t.Fatal("ordinary HTTP Dial enabled early data")
					}
				case <-ctx.Done():
					t.Fatal("ticket not published after SETTINGS", ctx.Err())
				}
			}
			if ct.ClientSessionCache != cache {
				t.Fatal("caller TLS cache replaced")
			}
		})
	}
}
