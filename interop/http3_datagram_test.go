package interop

import (
	"bytes"
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

// Test-only Extended CONNECT protocol: echo each HTTP datagram unchanged. This
// exercises the public client API and its session/lifetime integration.
func TestHTTPDatagramsIndependentServer(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, host := range []string{"127.0.0.1", "::1"} {
			t.Run(fmt.Sprintf("%x/%s", version, host), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				ct.NextProtos = []string{"h3"}
				st.NextProtos = []string{"h3"}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				handled := make(chan error, 1)
				payloads := [][]byte{{}, []byte("independent HTTP datagram"), bytes.Repeat([]byte{0x71}, 900)}
				server := &peerh3.Server{TLSConfig: st, EnableDatagrams: true, QUICConfig: &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != "CONNECT" || r.Proto != "datagram-test" {
						handled <- fmt.Errorf("unexpected request: %s %s", r.Method, r.Proto)
						return
					}
					stream := w.(peerh3.HTTPStreamer).HTTPStream()
					for _, want := range payloads {
						got, err := stream.ReceiveDatagram(ctx)
						if err != nil || !bytes.Equal(got, want) {
							handled <- fmt.Errorf("receive %x: %v", got, err)
							return
						}
						if err := stream.SendDatagram(got); err != nil {
							handled <- err
							return
						}
					}
					handled <- nil
					// Keep both stream directions open until the client has received all echoes.
					<-ctx.Done()
				})}
				socket, err := net.ListenPacket("udp", net.JoinHostPort(host, "0"))
				if err != nil {
					t.Fatal(err)
				}
				served := make(chan error, 1)
				go func() { served <- server.Serve(socket) }()
				defer func() { cancel(); server.Close(); socket.Close(); <-served }()

				quicConfig := &quic.Config{Versions: []quic.Version{version}}
				client, err := ownh3.Dial(ctx, "https://"+socket.LocalAddr().String(), ct, quicConfig, &ownh3.ClientConfig{EnableDatagrams: true, MaxConcurrentRequests: 1})
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				if quicConfig.EnableDatagrams {
					t.Fatal("HTTP Dial mutated caller QUIC configuration")
				}
				request, _ := http.NewRequestWithContext(ctx, "CONNECT", "https://"+socket.LocalAddr().String()+"/echo", nil)
				tunnel, response, err := client.OpenDatagramTunnel(request, "datagram-test")
				if err != nil {
					t.Fatal(err)
				}
				defer tunnel.Close()
				assertHTTP3BodySettings(t, ctx, response.Body, true, nil)
				if response.StatusCode != 200 || response.Body != tunnel {
					t.Fatal(response)
				}
				for _, payload := range payloads {
					if err := tunnel.SendDatagram(ctx, payload); err != nil {
						t.Fatal(err)
					}
					got, err := tunnel.ReceiveDatagram(ctx)
					if err != nil || !bytes.Equal(got, payload) {
						t.Fatal(got, err)
					}
				}
				select {
				case err := <-handled:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if err := tunnel.CloseWrite(); err != nil {
					t.Fatal(err)
				}
				if err := tunnel.SendDatagram(ctx, nil); err != ownh3.ErrDatagramSendClosed {
					t.Fatal("send survived FIN", err)
				}
				if err := tunnel.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := tunnel.ReceiveDatagram(ctx); err != ownh3.ErrDatagramRequestClosed {
					t.Fatal("receive survived close", err)
				}

			})
		}
	}
}

// Reverse role uses the public server API and upstream's actual HTTP/3 client.
// Two live request queues echo in reverse order; transport FIN ends reception.
func TestHTTPDatagramsIndependentClient(t *testing.T) { testHTTPDatagramsIndependentClient(t, false) }

func TestHTTPDatagramsSelectedServerPolicy(t *testing.T) { testHTTPDatagramsIndependentClient(t, true) }

func testHTTPDatagramsIndependentClient(t *testing.T, selected bool) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, host := range []string{"127.0.0.1", "::1"} {
			t.Run(fmt.Sprintf("%x/%s", version, host), func(t *testing.T) {
				ct, st := tlsConfigs(t)
				ct.NextProtos = []string{"h3"}
				st.NextProtos = []string{"h3"}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				cfg := &quic.Config{Versions: []quic.Version{version}, EnableDatagrams: true}
				if selected {
					cfg.EnableDatagrams = false
					cfg.GetConfigForClient = func(*quic.ClientInfo) (*quic.Config, error) {
						return &quic.Config{EnableDatagrams: true}, nil
					}
				}
				listener, err := quic.ListenAddr(net.JoinHostPort(host, "0"), st, cfg)
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				gates := []chan struct{}{make(chan struct{}), make(chan struct{})}
				payloads := [][]byte{[]byte("request zero"), bytes.Repeat([]byte{0x42}, 900)}
				results := make(chan error, 2)
				handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					index := 0
					if r.URL.Path == "/echo/1" {
						index = 1
					}
					if ownh3.ExtendedConnectProtocol(r) != "datagram-test" {
						results <- fmt.Errorf("wrong protocol")
						return
					}
					tunnel, err := ownh3.AcceptTunnel(w)
					if err != nil {
						results <- err
						return
					}
					data, err := tunnel.ReceiveDatagram(r.Context())
					if err != nil || !bytes.Equal(data, payloads[index]) {
						results <- fmt.Errorf("receive %d: %x %v", index, data, err)
						return
					}
					select {
					case <-gates[index]:
					case <-r.Context().Done():
						results <- r.Context().Err()
						return
					}
					if err := tunnel.SendDatagram(r.Context(), data); err != nil {
						results <- err
						return
					}
					// No application stream Read: transport FIN must end datagram reception.
					if _, err := tunnel.ReceiveDatagram(r.Context()); err != ownh3.ErrDatagramRequestClosed {
						results <- fmt.Errorf("receive survived FIN: %v", err)
						return
					}
					results <- tunnel.CloseWrite()
				})
				server := &ownh3.Server{MaxConcurrentRequests: 2, DatagramHandlers: map[string]http.Handler{"datagram-test": handler}}
				served := make(chan error, 1)
				go func() { served <- server.Serve(ctx, listener) }()
				defer func() { server.Close(); cancel(); <-served }()
				peer, err := dialUpstreamAddr(t, ctx, listener.Addr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, EnableDatagrams: true})
				if err != nil {
					t.Fatal(err)
				}
				defer peer.CloseWithError(0, "done")
				peerTransport := &peerh3.Transport{EnableDatagrams: true, DisableCompression: true}
				peerClient := peerTransport.NewClientConn(peer)
				var streams []*peerh3.RequestStream
				for i := 0; i < 2; i++ {
					stream, err := peerClient.OpenRequestStream(ctx)
					if err != nil {
						t.Fatal(err)
					}
					request, _ := http.NewRequestWithContext(ctx, "CONNECT", fmt.Sprintf("https://%s/echo/%d", listener.Addr(), i), nil)
					request.Proto = "datagram-test"
					if err := stream.SendRequestHeader(request); err != nil {
						t.Fatal(err)
					}
					response, err := stream.ReadResponse()
					if err != nil || response.StatusCode != 200 {
						t.Fatal(response, err)
					}
					streams = append(streams, stream)
				}
				for i, p := range payloads {
					if err := streams[i].SendDatagram(p); err != nil {
						t.Fatal(err)
					}
				}
				for i := 1; i >= 0; i-- {
					close(gates[i])
					data, err := streams[i].ReceiveDatagram(ctx)
					if err != nil || !bytes.Equal(data, payloads[i]) {
						t.Fatal("echo dispatch", i, data, err)
					}
				}
				for _, stream := range streams {
					if err := stream.Close(); err != nil {
						t.Fatal(err)
					}
					data, err := io.ReadAll(stream)
					if err != nil || len(data) != 0 {
						t.Fatal(data, err)
					}
				}
				for range streams {
					select {
					case err := <-results:
						if err != nil {
							t.Fatal(err)
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
			})
		}
	}
}
