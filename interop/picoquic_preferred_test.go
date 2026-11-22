package interop

import (
	"bytes"
	"context"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
)

func TestPicoquicPreferredAddress(t *testing.T) {
	binary := os.Getenv("PICOQUIC_DEMO")
	if binary == "" {
		t.Skip("set PICOQUIC_DEMO to the pinned external peer binary")
	}
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		for _, host := range []string{"127.0.0.1", "::1"} {
			t.Run(fmt.Sprintf("%x/%s", version, host), func(t *testing.T) {
				_, st := tlsConfigs(t)
				st.NextProtos = []string{"hq-interop"}
				address := netip.AddrPortFrom(netip.MustParseAddr(host), 0)
				listener, err := quic.ListenAddr(address.String(), st, &quic.Config{Versions: []quic.Version{version}, PreferredAddresses: []netip.AddrPort{address}})
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				dir := t.TempDir()
				cert := filepath.Join(dir, "root.pem")
				if err := os.WriteFile(cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: st.Certificates[0].Certificate[0]}), 0600); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, binary, "-a", "hq-interop", "-n", "localhost", "-t", cert, "-v", fmt.Sprintf("%x", uint32(version)), "-o", dir, host, strconv.Itoa(listener.Addr().(*net.UDPAddr).Port), "payload.bin")
				cmd.Dir = dir
				var log bytes.Buffer
				cmd.Stdout = &log
				cmd.Stderr = &log
				if err := cmd.Start(); err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() { done <- cmd.Wait() }()
				joined := false
				defer func() {
					if !joined {
						cmd.Process.Kill()
						<-done
					}
					if t.Failed() {
						t.Log(log.String())
					}
				}()
				server, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				stream, err := server.AcceptStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				deadline, _ := ctx.Deadline()
				stream.SetDeadline(deadline)
				request, err := io.ReadAll(io.LimitReader(stream, 1024))
				if err != nil || string(request) != "GET /payload.bin\r\n" {
					t.Fatalf("request %q: %v", request, err)
				}
				payload := make([]byte, 64*1024)
				for i := range payload {
					payload[i] = byte(i*31 + i/251)
				}
				if _, err := stream.Write(payload[:1024]); err != nil {
					t.Fatal(err)
				}
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				for server.LocalAddr().String() != listener.PreferredAddresses()[0].String() {
					select {
					case <-ctx.Done():
						t.Fatal("preferred migration did not commit", ctx.Err())
					case err := <-done:
						joined = true
						t.Fatal("peer exited before migration", err)
					case <-ticker.C:
					}
				}
				if _, err := stream.Write(payload[1024:]); err != nil {
					t.Fatal(err)
				}
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-done:
					joined = true
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				got, err := os.ReadFile(filepath.Join(dir, "payload.bin"))
				if err != nil || !bytes.Equal(got, payload) {
					t.Fatal("download mismatch", len(got), err)
				}
			})
		}
	}

}
