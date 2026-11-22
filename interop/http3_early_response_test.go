package interop

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

type earlyUploadBody struct {
	reader          *io.PipeReader
	entered, closed chan struct{}
	once            sync.Once
	closes          atomic.Int32
}

func (b *earlyUploadBody) Read(p []byte) (int, error) {
	b.once.Do(func() { close(b.entered) })
	return b.reader.Read(p)
}
func (b *earlyUploadBody) Close() error {
	if b.closes.Add(1) == 1 {
		close(b.closed)
	}
	return b.reader.Close()
}
func TestHTTP3EarlyResponseStopsBlockedUpload(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", uint32(version)), func(t *testing.T) {
			clientTLS, serverTLS := tlsConfigs(t)
			clientTLS.NextProtos = []string{"h3"}
			serverTLS.NextProtos = []string{"h3"}
			reader, producer := io.Pipe()
			defer reader.Close()
			defer producer.Close()
			body := &earlyUploadBody{reader: reader, entered: make(chan struct{}), closed: make(chan struct{})}
			server := &peerh3.Server{TLSConfig: serverTLS, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-body.entered:
				case <-r.Context().Done():
					return
				}
				w.WriteHeader(403)
				w.Write([]byte("early rejection"))
			})}
			socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			served := make(chan struct{})
			go func() { defer close(served); server.Serve(socket) }()
			defer func() {
				server.Close()
				socket.Close()
				select {
				case <-served:
				case <-time.After(time.Second):
					t.Error("peer server did not join")
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			origin := "https://" + socket.LocalAddr().String()
			// Dial must preserve explicit TLS verification, clone ALPN, and
			// detach the successful connection from the setup-only context.
			badTLS := clientTLS.Clone()
			badTLS.ServerName = "wrong.example"
			if rejected, err := ownh3.Dial(ctx, origin, badTLS, &quic.Config{Versions: []quic.Version{version}}, nil); err == nil {
				rejected.Close()
				t.Fatal("hostname verification bypassed")
			} else if !strings.Contains(err.Error(), "wrong.example") {
				t.Fatal("dial failed without hostname-verification evidence", err)
			}
			clientTLS.NextProtos = []string{"caller-alpn"}
			dialCtx, stopDial := context.WithCancel(ctx)
			client, err := ownh3.Dial(dialCtx, origin, clientTLS, &quic.Config{Versions: []quic.Version{version}}, nil)
			stopDial()
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			if len(clientTLS.NextProtos) != 1 || clientTLS.NextProtos[0] != "caller-alpn" {
				t.Fatal("caller TLS configuration mutated")
			}
			request, _ := http.NewRequestWithContext(ctx, "POST", origin+"/reject", body)
			response, err := client.RoundTrip(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			// Require peer STOP_SENDING to stop the upload before local response closure.
			select {
			case <-body.closed:
			case <-ctx.Done():
				t.Fatal("peer did not unblock upload", ctx.Err())
			}
			payload, err := io.ReadAll(response.Body)
			if err != nil || response.StatusCode != 403 || string(payload) != "early rejection" {
				t.Fatal(response.StatusCode, string(payload), err)
			}
			if err := response.Body.Close(); err != nil {
				t.Fatal(err)
			}
			if body.closes.Load() != 1 {
				t.Fatal("upload closed more than once", body.closes.Load())
			}
		})
	}
}
