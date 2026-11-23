package interop

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

func TestHTTP3ClientConnectionContext(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, local := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/local=%v", v, local), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				ct, st := tlsConfigs(t)
				ct.NextProtos = []string{"h3"}
				st.NextProtos = []string{"h3"}
				listener, err := upstream.ListenAddr("127.0.0.1:0", st, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}})
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				peers := make(chan *upstream.Conn, 1)
				server := &peerh3.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "still usable") })}
				done := make(chan struct{})
				go func() {
					defer close(done)
					peer, err := listener.Accept(ctx)
					if err != nil {
						return
					}
					peers <- peer
					server.ServeQUICConn(peer)
				}()
				defer func() { server.Close(); listener.Close(); <-done }()
				origin := "https://" + listener.Addr().String()
				client, err := ownh3.Dial(ctx, origin, ct, &quic.Config{Versions: []quic.Version{v}}, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				lifetime := client.Context()
				if lifetime != client.Context() || lifetime.Err() != nil {
					t.Fatal("unstable or closed context")
				}
				if err := client.CloseWithError(1<<62, "invalid"); err == nil {
					t.Fatal("invalid close accepted")
				}
				req, _ := http.NewRequestWithContext(ctx, "GET", origin, nil)
				response, err := client.RoundTrip(req)
				if err != nil {
					t.Fatal("invalid close damaged connection", err)
				}
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil || string(body) != "still usable" {
					t.Fatal(string(body), err)
				}
				var peer *upstream.Conn
				select {
				case peer = <-peers:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				const code = 0xface
				const message = "explicit HTTP connection close"
				if local {
					err = client.CloseWithError(code, message)
				} else {
					err = peer.CloseWithError(code, message)
				}
				if err != nil {
					t.Fatal(err)
				}
				select {
				case <-lifetime.Done():
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				var ownError *quic.ApplicationError
				if cause := context.Cause(lifetime); !errors.As(cause, &ownError) || ownError.ErrorCode != code || ownError.ErrorMessage != message || ownError.Remote == local {
					t.Fatalf("own cause %T %v", cause, cause)
				}
				select {
				case <-peer.Context().Done():
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				var peerError *upstream.ApplicationError
				if cause := context.Cause(peer.Context()); !errors.As(cause, &peerError) || peerError.ErrorCode != code || peerError.ErrorMessage != message || peerError.Remote != local {
					t.Fatalf("peer cause %T %v", cause, cause)
				}
				client.Close()
				if context.Cause(client.Context()) != context.Cause(lifetime) {
					t.Fatal("cleanup replaced close cause")
				}
			})
		}
	}
}
