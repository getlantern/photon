package interop

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

func TestTunnelPeerResetWriteCause(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			ct, st := tlsConfigs(t)
			ct.NextProtos = []string{"h3"}
			st.NextProtos = []string{"h3"}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			reset := make(chan struct{})
			server := &peerh3.Server{TLSConfig: st, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(200)
				w.(http.Flusher).Flush()
				stream := w.(peerh3.HTTPStreamer).HTTPStream()
				select {
				case <-reset:
				case <-r.Context().Done():
					return
				}
				stream.CancelRead(0x10c)
				stream.CancelWrite(0x10c)
			})}
			socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() { defer close(done); server.Serve(socket) }()
			defer func() { server.Close(); socket.Close(); <-done }()
			origin := "https://" + socket.LocalAddr().String()
			client, err := ownh3.Dial(ctx, origin, ct, &quic.Config{Versions: []quic.Version{version}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			request, _ := http.NewRequestWithContext(ctx, "CONNECT", origin, nil)
			request.Host = "target.example:443"
			tunnel, _, err := client.OpenTunnel(request)
			if err != nil {
				t.Fatal(err)
			}
			defer tunnel.Close()
			close(reset)
			_, err = tunnel.Read(make([]byte, 1))
			var receive *quic.StreamError
			if !errors.As(err, &receive) || !receive.Remote || receive.ErrorCode != 0x10c {
				t.Fatal("read reset", err)
			}
			for {
				_, err = tunnel.Write([]byte("x"))
				if err != nil {
					break
				}
				if ctx.Err() != nil {
					t.Fatal("write never observed reset", ctx.Err())
				}
			}
			var resetErr *quic.StreamError
			var h3err *ownh3.Error
			if !errors.As(err, &resetErr) || !resetErr.Remote || resetErr.ErrorCode != 0x10c || !errors.As(err, &h3err) || h3err.ErrorCode != ownh3.ErrCodeRequestCanceled {
				t.Fatal("write reset cause", err)
			}
		})
	}
}
