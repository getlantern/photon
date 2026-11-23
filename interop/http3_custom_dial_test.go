package interop

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

func TestHTTP3CustomDialMigration(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ct, st := tlsConfigs(t)
			socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer socket.Close()
			server := &peerh3.Server{TLSConfig: st, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, r.RemoteAddr) })}
			served := make(chan struct{})
			go func() { defer close(served); server.Serve(socket) }()
			defer func() {
				server.Close()
				select {
				case <-served:
				case <-time.After(time.Second):
					t.Error("server did not join")
				}
			}()
			makeTransport := func() *quic.Transport {
				socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				transport := &quic.Transport{Conn: socket}
				t.Cleanup(func() { transport.Close(); socket.Close() })
				return transport
			}
			primary, alternate := makeTransport(), makeTransport()
			connected := make(chan *quic.Conn, 1)
			var calls atomic.Int32
			client := &ownh3.Transport{TLSClientConfig: ct, QUICConfig: &quic.Config{Versions: []quic.Version{version}},
				Dial: func(ctx context.Context, address string, tlsConfig *tls.Config, config *quic.Config) (*quic.Conn, error) {
					calls.Add(1)
					if address != "localhost:443" || tlsConfig.ServerName != "localhost" || tlsConfig.NextProtos[0] != "h3" {
						return nil, fmt.Errorf("incorrect dial arguments")
					}
					conn, err := primary.Dial(ctx, socket.LocalAddr(), tlsConfig, config)
					if err == nil {
						connected <- conn
					}
					return conn, err
				}}
			defer client.Close()
			get := func() string {
				request, _ := http.NewRequestWithContext(ctx, "GET", "https://localhost/", nil)
				response, err := client.RoundTrip(request)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil || response.StatusCode != 200 {
					t.Fatal("response", response.StatusCode, err)
				}
				return string(body)
			}
			before := get()
			conn := <-connected
			if before != primary.Conn.LocalAddr().String() {
				t.Fatal("initial source", before)
			}
			path, err := conn.AddPath(alternate)
			if err != nil {
				t.Fatal(err)
			}
			defer path.Close()
			if err := path.Probe(ctx); err != nil {
				t.Fatal(err)
			}
			if err := path.Switch(); err != nil {
				t.Fatal(err)
			}
			if after := get(); after != alternate.Conn.LocalAddr().String() {
				t.Fatal("migrated source", after)
			}
			if calls.Load() != 1 {
				t.Fatal("migration redialed HTTP connection", calls.Load())
			}
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-conn.Context().Done():
			case <-ctx.Done():
				t.Fatal("HTTP transport did not close supplied connection")
			}
		})
	}
}
