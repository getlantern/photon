package interop

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

// Completed local TLS does not erase replay exposure inherited from another hop.
// Reuse one request slot after denial, including a fresh datagram association.
func TestHTTP3ExtensionReplayPolicy(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, datagrams := range []bool{false, true} {
			for _, policy := range []string{"default", "deny", "allow"} {
				t.Run(fmt.Sprintf("%x/datagrams=%t/%s", version, datagrams, policy), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					ct, st := tlsConfigs(t)
					ct.NextProtos, st.NextProtos = []string{"h3"}, []string{"h3"}
					listener, err := quic.ListenAddr("127.0.0.1:0", st, &quic.Config{Versions: []quic.Version{version}, EnableDatagrams: datagrams})
					if err != nil {
						t.Fatal(err)
					}
					defer listener.Close()
					var handled, checked atomic.Int32
					results := make(chan error, 2)
					handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						handled.Add(1)
						tunnel, err := ownh3.AcceptTunnel(w)
						if err != nil {
							results <- err
							return
						}
						if datagrams {
							var data []byte
							data, err = tunnel.ReceiveDatagram(r.Context())
							if err == nil && string(data) != "probe" {
								err = fmt.Errorf("unexpected datagram %q", data)
							}
							if err == nil {
								err = tunnel.SendDatagram(r.Context(), data)
							}
						}
						if err == nil {
							_, err = io.Copy(tunnel, tunnel)
						}
						results <- err
					})
					server := &ownh3.Server{MaxConcurrentRequests: 1}
					if datagrams {
						server.DatagramHandlers = map[string]http.Handler{"replay-test": handler}
					} else {
						server.ExtendedConnectHandlers = map[string]http.Handler{"replay-test": handler}
					}
					if policy != "default" {
						server.AllowEarlyData = func(r *http.Request) bool {
							checked.Add(1)
							if r.TLS == nil || !r.TLS.HandshakeComplete || r.Header.Get("Early-Data") != "1" || ownh3.ExtendedConnectProtocol(r) != "replay-test" {
								t.Error("policy received incorrect TLS, replay or protocol metadata")
							}
							return policy == "allow"
						}
					}
					served := make(chan error, 1)
					go func() { served <- server.Serve(ctx, listener) }()
					defer func() { server.Close(); cancel(); <-served }()
					peer, err := dialUpstreamAddr(t, ctx, listener.Addr().String(), ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, EnableDatagrams: datagrams})
					if err != nil {
						t.Fatal(err)
					}
					defer peer.CloseWithError(0, "done")
					transport := &peerh3.Transport{EnableDatagrams: datagrams}
					client := transport.NewClientConn(peer)
					for _, forwarded := range []bool{true, false} {
						stream, err := client.OpenRequestStream(ctx)
						if err != nil {
							t.Fatal(err)
						}
						request, _ := http.NewRequestWithContext(ctx, http.MethodConnect, "https://"+listener.Addr().String()+"/tunnel", nil)
						request.Proto = "replay-test"
						if forwarded {
							request.Header["Early-Data"] = []string{"0", "invalid"}
						}
						if err := stream.SendRequestHeader(request); err != nil {
							t.Fatal(err)
						}
						response, err := stream.ReadResponse()
						if err != nil {
							t.Fatal(err)
						}
						if forwarded && policy != "allow" {
							if response.StatusCode != http.StatusTooEarly {
								t.Fatal("expected 425", response.StatusCode)
							}
							if _, err := io.ReadAll(stream); err != nil {
								t.Fatal(err)
							}
							stream.Close()
							if handled.Load() != 0 {
								t.Fatal("denied request reached extension handler")
							}
							continue
						}
						if response.StatusCode != http.StatusOK {
							t.Fatal("expected tunnel", response.StatusCode)
						}
						if datagrams {
							if err := stream.SendDatagram([]byte("probe")); err != nil {
								t.Fatal(err)
							}
							data, err := stream.ReceiveDatagram(ctx)
							if err != nil || string(data) != "probe" {
								t.Fatal("datagram echo", string(data), err)
							}
						}
						if _, err := stream.Write([]byte("stream probe")); err != nil {
							t.Fatal(err)
						}
						if err := stream.Close(); err != nil {
							t.Fatal(err)
						}
						data, err := io.ReadAll(stream)
						if err != nil || string(data) != "stream probe" {
							t.Fatal("stream echo", string(data), err)
						}
						select {
						case err := <-results:
							if err != nil {
								t.Fatal(err)
							}
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						}
					}
					wantHandled := int32(1)
					if policy == "allow" {
						wantHandled = 2
					}
					wantChecked := int32(1)
					if policy == "default" {
						wantChecked = 0
					}
					if handled.Load() != wantHandled || checked.Load() != wantChecked {
						t.Fatalf("handled=%d checked=%d", handled.Load(), checked.Load())
					}
				})
			}
		}
	}
}
