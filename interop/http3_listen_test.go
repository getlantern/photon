package interop

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	ownh3 "github.com/getlantern/photon/http3"
	upstream "github.com/quic-go/quic-go"
	peerh3 "github.com/quic-go/quic-go/http3"
)

func TestHTTP3ListenAndServeIndependent(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, files := range []bool{false, true} {
			for _, graceful := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/files=%v/graceful=%v", version, files, graceful), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					ct, st := tlsConfigs(t)
					ct.NextProtos = []string{peerh3.NextProtoH3}
					// The convenience API binds its own socket. Reserve an ephemeral address
					// briefly; any competing bind is reported by the serve result below.
					reservation, err := net.ListenPacket("udp4", "127.0.0.1:0")
					if err != nil {
						t.Fatal(err)
					}
					address := reservation.LocalAddr().String()
					reservation.Close()
					cfg := &quic.Config{Versions: []quic.Version{version}}
					release := make(chan struct{})
					var server *ownh3.Server
					server = &ownh3.Server{Addr: address, TLSConfig: st, QUICConfig: cfg, EnableDatagrams: true,
						Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							if r.ProtoMajor != 3 || r.TLS == nil || r.TLS.NegotiatedProtocol != ownh3.NextProtoH3 {
								http.Error(w, "protocol mismatch", 500)
								return
							}
							if err := server.SetQUICHeaders(w.Header()); err != nil {
								http.Error(w, err.Error(), 500)
								return
							}
							io.WriteString(w, "independent ")
							if graceful {
								w.(http.Flusher).Flush()
								select {
								case <-release:
								case <-r.Context().Done():
									return
								}
							}
							io.WriteString(w, "startup")
						})}
					certFile, keyFile := filepath.Join(t.TempDir(), "cert.pem"), filepath.Join(t.TempDir(), "key.pem")
					if files {
						certificate := st.Certificates[0]
						key, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
						if err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]}), 0600); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0600); err != nil {
							t.Fatal(err)
						}
					}
					done := make(chan struct{})
					var serveErr error
					go func() {
						defer close(done)
						if files {
							serveErr = server.ListenAndServeTLS(certFile, keyFile)
						} else {
							serveErr = server.ListenAndServe()
						}
					}()
					defer func() {
						server.Close()
						select {
						case <-done:
						case <-time.After(time.Second):
							t.Error("startup worker did not join")
						}
					}()
					peer, err := dialUpstreamAddr(t, ctx, address, ct, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, EnableDatagrams: true})
					if err != nil {
						select {
						case <-done:
							t.Fatalf("serve: %v; dial: %v", serveErr, err)
						default:
							t.Fatal(err)
						}
					}
					defer peer.CloseWithError(0, "")
					transport := &peerh3.Transport{EnableDatagrams: true}
					defer transport.Close()
					client := transport.NewClientConn(peer)
					request, _ := http.NewRequestWithContext(ctx, "GET", "https://"+address+"/", nil)
					response, err := client.RoundTrip(request)
					if err != nil {
						t.Fatal(err)
					}
					defer response.Body.Close()
					_, port, _ := net.SplitHostPort(address)
					if response.Header.Get("Alt-Svc") != fmt.Sprintf(`h3=":%s"; ma=2592000`, port) {
						t.Fatal("listener advertisement", response.Header)
					}
					if graceful {
						short, stop := context.WithTimeout(ctx, 20*time.Millisecond)
						err := server.Shutdown(short)
						stop()
						if !errors.Is(err, context.DeadlineExceeded) {
							t.Fatal("shutdown lost active handler", err)
						}
						select {
						case <-done:
							t.Fatal("owned listener returned before response drain", serveErr)
						default:
						}
						close(release)
					}
					body, err := io.ReadAll(response.Body)
					response.Body.Close()
					if err != nil || response.StatusCode != 200 || string(body) != "independent startup" {
						t.Fatalf("response %d %q: %v", response.StatusCode, body, err)
					}
					if !peer.ConnectionState().SupportsDatagrams.Remote {
						t.Fatal("QUIC datagrams not enabled")
					}
					peer.CloseWithError(0, "")
					if graceful {
						err = server.Shutdown(ctx)
					} else {
						err = server.Close()
					}
					if err != nil {
						t.Fatal(err)
					}
					select {
					case <-done:
						if !errors.Is(serveErr, http.ErrServerClosed) {
							t.Fatal("serve result", serveErr)
						}
					case <-ctx.Done():
						t.Fatal("server did not stop", ctx.Err())
					}
					rebound, err := net.ListenPacket("udp4", address)
					if err != nil {
						t.Fatal("owned socket retained", err)
					}
					rebound.Close()
					if cfg.EnableDatagrams || cfg.Allow0RTT || st.NextProtos[0] != "independent-quic-test" {
						t.Fatal("caller configuration mutated")
					}
				})
			}
		}
	}
}
