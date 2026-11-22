package interop

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
)

func TestPicoquicPreferredServer(t *testing.T) {
	binary := os.Getenv("PICOQUIC_DEMO")
	if binary == "" {
		t.Skip("set PICOQUIC_DEMO to the pinned external peer binary")
	}
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, host := range []string{"127.0.0.1", "::1"} {
			for _, loss := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/%s/loss=%v", version, host, loss), func(t *testing.T) {
					ct, st := tlsConfigs(t)
					ct.NextProtos = []string{"hq-interop"}
					dir := t.TempDir()
					key, err := x509.MarshalPKCS8PrivateKey(st.Certificates[0].PrivateKey)
					if err != nil {
						t.Fatal(err)
					}
					write := func(name string, data []byte) {
						t.Helper()
						if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
							t.Fatal(err)
						}
					}
					write("cert.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: st.Certificates[0].Certificate[0]}))
					write("key.pem", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}))
					payload := make([]byte, 64*1024)
					for i := range payload {
						payload[i] = byte(i*17 + i/239)
					}
					write("payload.bin", payload)
					network, option := "udp4", "-4"
					if host == "::1" {
						network, option = "udp6", "-6"
					}
					strict := os.Getenv("QUIC_INTEROP_KNOWN_FAILURES") == "1"
					reservation, err := net.ListenUDP(network, &net.UDPAddr{IP: net.ParseIP(host)})
					if err != nil {
						t.Fatal(err)
					}
					address := reservation.LocalAddr().(*net.UDPAddr)
					initialSocket, err := net.ListenUDP(network, &net.UDPAddr{IP: net.ParseIP(host)})
					if err != nil {
						reservation.Close()
						t.Fatal(err)
					}
					initial := initialSocket.LocalAddr().(*net.UDPAddr)
					initialSocket.Close()
					reservation.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()
					args := []string{"-a", "hq-interop", "-p", fmt.Sprintf("%d:%d", initial.Port, address.Port), "-c", filepath.Join(dir, "cert.pem"), "-k", filepath.Join(dir, "key.pem"), "-w", dir, "-q", dir}
					if strict {
						args = append(args, option, host)
					} else {
						// The pinned CLI accepts bare IPs and uses the second -p port.
						// Its encoder uses the IPv4 port field for both families, so
						// advertise both; strict mode retains the IPv6-only failure.
						args = append(args, "-4", "127.0.0.1", "-6", "::1")
					}
					cmd := exec.CommandContext(ctx, binary, args...)
					cmd.Dir = dir
					var log bytes.Buffer
					cmd.Stdout = &log
					cmd.Stderr = &log
					if err := cmd.Start(); err != nil {
						t.Fatal(err)
					}
					defer func() {
						cmd.Process.Kill()
						cmd.Wait()
						if t.Failed() {
							t.Log(log.String())
							files, _ := filepath.Glob(filepath.Join(dir, "*.qlog"))
							for _, file := range files {
								data, _ := os.ReadFile(file)
								t.Logf("peer qlog: %s", data[:min(len(data), 16000)])
							}
						}
					}()
					dialAddress := initial
					var drops *atomic.Uint32
					if loss {
						dialAddress, drops = preferredLossRelay(t, network, initial)
					}
					client, err := quic.DialAddr(ctx, dialAddress.String(), ct, &quic.Config{Versions: []quic.Version{version}, LocalAddr: &net.UDPAddr{IP: net.ParseIP(host)}})
					if err != nil {
						t.Fatal(err)
					}
					defer client.CloseWithError(0, "")
					ticker := time.NewTicker(time.Millisecond)
					defer ticker.Stop()
					for client.RemoteAddr().String() != address.String() {
						select {
						case <-ctx.Done():
							t.Fatal("client preferred switch", ctx.Err())
						case <-client.Context().Done():
							t.Fatal(context.Cause(client.Context()))
						case <-ticker.C:
						}
					}
					stream, err := client.OpenStreamSync(ctx)
					if err != nil {
						t.Fatal(err)
					}
					deadline, _ := ctx.Deadline()
					stream.SetDeadline(deadline)
					if _, err := stream.Write([]byte("GET /payload.bin\r\n")); err != nil {
						t.Fatal(err)
					}
					if err := stream.Close(); err != nil {
						t.Fatal(err)
					}
					if loss && drops.Load() != 2 {
						t.Fatal("loss fixture did not drop both directions")
					}
					got, err := io.ReadAll(stream)
					if err != nil || !bytes.Equal(got, payload) {
						t.Fatal("preferred download", len(got), err)
					}
				})
			}
		}
	}
}
