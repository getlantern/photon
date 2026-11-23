package http3

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
)

func TestListenAndServeOwnership(t *testing.T) {
	origin := httptest.NewTLSServer(nil)
	credentials := origin.TLS.Clone()
	origin.Close()
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	cert := credentials.Certificates[0]
	key, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), 0600); err != nil {
		t.Fatal(err)
	}
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, files := range []bool{false, true} {
			for _, graceful := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/files=%v/graceful=%v", version, files, graceful), func(t *testing.T) {
					source := credentials.Clone()
					source.NextProtos = []string{"application-original"}
					cfg := &quic.Config{Versions: []quic.Version{version}}
					server := &Server{Addr: "127.0.0.1:0", TLSConfig: source, QUICConfig: cfg, EnableDatagrams: true, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.ProtoMajor != 3 || r.TLS == nil || r.TLS.NegotiatedProtocol != NextProtoH3 {
							http.Error(w, "protocol mismatch", 500)
							return
						}
						io.WriteString(w, "served")
					})}
					done := make(chan error, 1)
					go func() {
						if files {
							done <- server.ListenAndServeTLS(certFile, keyFile)
						} else {
							done <- server.ListenAndServe()
						}
					}()
					defer server.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					defer cancel()
					var address string
					for address == "" {
						server.mu.Lock()
						for entry := range server.listeners {
							address = entry.listener.Addr().String()
						}
						server.mu.Unlock()
						if address != "" {
							break
						}
						select {
						case err := <-done:
							t.Fatal("startup failed", err)
						case <-ctx.Done():
							t.Fatal(ctx.Err())
						case <-time.After(time.Millisecond):
						}
					}
					client := &Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, QUICConfig: &quic.Config{Versions: []quic.Version{version}, EnableDatagrams: true}, EnableDatagrams: true}
					defer client.Close()
					request, _ := http.NewRequestWithContext(ctx, "GET", "https://"+address+"/", nil)
					response, err := client.RoundTrip(request)
					if err != nil {
						t.Fatal(err)
					}
					body, err := io.ReadAll(response.Body)
					response.Body.Close()
					if err != nil || response.StatusCode != 200 || string(body) != "served" {
						t.Fatal("response", response.StatusCode, string(body), err)
					}
					client.Close()
					if graceful {
						if err := server.Shutdown(ctx); err != nil {
							t.Fatal(err)
						}
					} else {
						if err := server.Close(); err != nil {
							t.Fatal(err)
						}
					}
					select {
					case err := <-done:
						if !errors.Is(err, ErrServerClosed) {
							t.Fatal("serve result", err)
						}
					case <-ctx.Done():
						t.Fatal("server did not exit", ctx.Err())
					}
					rebound, err := net.ListenPacket("udp4", address)
					if err != nil {
						t.Fatal("owned socket retained", err)
					}
					rebound.Close()
					if cfg.EnableDatagrams || cfg.Allow0RTT || source.NextProtos[0] != "application-original" {
						t.Fatal("caller configuration mutated")
					}
				})
			}
		}
	}
}

func TestListenAndServePreflight(t *testing.T) {
	for _, server := range []*Server{{Addr: "127.0.0.1:0"}, {Addr: "127.0.0.1:0", TLSConfig: &tls.Config{}, MaxHeaderBytes: -1}} {
		if err := server.ListenAndServe(); err == nil {
			server.Close()
			t.Fatal("invalid configuration accepted")
		}
		if len(server.listeners) != 0 {
			t.Fatal("failed preflight registered listener")
		}
	}
	server := &Server{Addr: "127.0.0.1:0", TLSConfig: &tls.Config{}}
	server.Close()
	if err := server.ListenAndServe(); !errors.Is(err, ErrServerClosed) {
		t.Fatal("closed server restarted", err)
	}
	if err := ListenAndServeQUIC("127.0.0.1:0", filepath.Join(t.TempDir(), "missing-cert"), "missing-key", nil); err == nil {
		t.Fatal("missing file accepted")
	}
}
