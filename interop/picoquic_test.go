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
	"strconv"
	"testing"
	"time"

	independent "github.com/getlantern/photon"
)

// This external-peer gate is opt-in; see picoquic/README.md for the pinned build.
func TestPicoquicMultipath(t *testing.T)           { testPicoquicMultipath(t, false, false) }
func TestPicoquicGreasedMultipath(t *testing.T)    { testPicoquicMultipath(t, true, false) }
func TestPicoquicCompatibleMultipath(t *testing.T) { testPicoquicMultipath(t, true, true) }
func testPicoquicMultipath(t *testing.T, grease, compatible bool) {
	for _, host := range []string{"127.0.0.1", "::1"} {
		t.Run(host, func(t *testing.T) { testPicoquicMultipathHost(t, grease, compatible, host) })
	}
}

func testPicoquicMultipathHost(t *testing.T, grease, compatible bool, host string) {
	binary := os.Getenv("PICOQUIC_DEMO")
	if binary == "" {
		t.Skip("set PICOQUIC_DEMO to the pinned external peer binary")
	}
	for _, version := range []independent.Version{independent.Version1, independent.Version2} {
		if compatible && version == independent.Version2 && os.Getenv("QUIC_INTEROP_KNOWN_FAILURES") != "1" {
			t.Run("6b3343cf", func(t *testing.T) { t.Skip("pinned Picoquic lacks v2-to-v1 transition; see KNOWN_FAILURES.md") })
			continue
		}
		for _, loss := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/loss=%t", uint32(version), loss), func(t *testing.T) {
				clientTLS, serverTLS := tlsConfigs(t)
				clientTLS.NextProtos = []string{"hq-interop"}
				dir := t.TempDir()
				key, err := x509.MarshalPKCS8PrivateKey(serverTLS.Certificates[0].PrivateKey)
				if err != nil {
					t.Fatal(err)
				}
				write := func(name string, data []byte) {
					t.Helper()
					if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
						t.Fatal(err)
					}
				}
				write("cert.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverTLS.Certificates[0].Certificate[0]}))
				write("key.pem", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}))
				payload := bytes.Repeat([]byte("independent multipath peer\n"), 32)
				write("payload.txt", payload)
				largePayload := make([]byte, 1<<20)
				for i := range largePayload {
					largePayload[i] = byte(i*31 + i/251)
				}
				write("large.bin", largePayload)
				socket, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP(host)})
				if err != nil {
					t.Fatal(err)
				}
				address := socket.LocalAddr().(*net.UDPAddr)
				socket.Close()
				log, err := os.Create(filepath.Join(dir, "peer.log"))
				if err != nil {
					t.Fatal(err)
				}
				cmd := exec.Command(binary, "-M", "-a", "hq-interop", "-p", strconv.Itoa(address.Port), "-c", filepath.Join(dir, "cert.pem"), "-k", filepath.Join(dir, "key.pem"), "-w", dir, "-m", "1200")
				cmd.Stdout, cmd.Stderr = log, log
				if err := cmd.Start(); err != nil {
					log.Close()
					t.Fatal(err)
				}
				defer func() {
					cmd.Process.Kill()
					cmd.Wait()
					log.Close()
					if t.Failed() {
						data, _ := os.ReadFile(filepath.Join(dir, "peer.log"))
						t.Logf("picoquic: %s", data)
					}
				}()
				// Distinct server-facing bindings ensure the peer observes each move.
				relay := newMigrationRelay(t, address, loss, loss)
				defer relay.close()
				address = relay.front.LocalAddr().(*net.UDPAddr)
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				selected := version
				versions := []independent.Version{version}
				if compatible {
					selected = independent.Version1
					if selected == version {
						selected = independent.Version2
					}
					versions = []independent.Version{selected, version}
				}
				client, err := independent.DialAddr(ctx, address.String(), clientTLS, &independent.Config{InitialVersion: version, EnableCompatibleVersionNegotiation: compatible, Versions: versions, EnableMultipath: true, EnableQUICBitGreasing: grease, LocalAddr: &net.UDPAddr{IP: net.ParseIP(host)}})
				if err != nil {
					t.Fatal(err)
				}
				defer client.CloseWithError(0, "")
				if relay.clientFirstVersion.Load() != uint32(version) {
					t.Fatal("client did not send configured original version")
				}
				if compatible && relay.versionNegotiations.Load() != 0 {
					t.Fatal("incompatible negotiation used instead of switching")
				}
				if client.ConnectionState().Version != selected {
					t.Fatalf("negotiated version=%x want=%x", client.ConnectionState().Version, selected)
				}
				if !client.ConnectionState().SupportsMultipath {
					t.Fatal("peer did not negotiate multipath")
				}
				path, err := client.OpenPath(ctx, 1, &net.UDPAddr{IP: net.ParseIP(host)}, address)
				if err != nil {
					t.Fatal(err)
				}
				if err := client.AbandonPath(ctx, 0); err != nil {
					t.Fatal(err)
				}
				if path.ID() != 1 {
					t.Fatal("wrong path ID")
				}
				fetch := func(name string, want []byte, during func()) {
					t.Helper()
					clientSet, clientClear := relay.clientBits.set.Load(), relay.clientBits.clear.Load()
					serverSet, serverClear := relay.serverBits.set.Load(), relay.serverBits.clear.Load()
					stream, err := client.OpenStreamSync(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if err := stream.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
						t.Fatal(err)
					}
					if _, err := stream.Write([]byte("GET /" + name + "\r\n")); err != nil {
						t.Fatal(err)
					}
					if err := stream.Close(); err != nil {
						t.Fatal(err)
					}
					var prefix []byte
					if during != nil {
						prefix = make([]byte, 1024)
						if _, err := io.ReadFull(stream, prefix); err != nil {
							t.Fatal(err)
						}
						if !bytes.Equal(prefix, want[:len(prefix)]) {
							t.Fatal("wrong prefix before migration")
						}
						// The default 512 KiB stream receive window advances only with
						// consumption, which this application pauses to migrate, so the
						// 1 MiB response cannot complete first.
						during()
					}
					// Bound a malformed peer's response while still requiring stream EOF.
					rest, err := io.ReadAll(io.LimitReader(stream, int64(len(want)-len(prefix)+1)))
					received := append(prefix, rest...)
					if err != nil || !bytes.Equal(received, want) {
						t.Fatalf("%s: response len=%d want=%d: %v", name, len(received), len(want), err)
					}
					if grease && len(want) >= 1<<20 && (relay.clientBits.set.Load() == clientSet || relay.clientBits.clear.Load() == clientClear || relay.serverBits.set.Load() == serverSet || relay.serverBits.clear.Load() == serverClear) {
						t.Fatal("large transfer did not exercise both bits in both directions")
					}
				}
				fetch("payload.txt", payload, nil)
				fetch("large.bin", largePayload, nil)
				migrate := func() {
					t.Helper()
					before := path.LocalAddr().String()
					if err := path.Migrate(ctx, &net.UDPAddr{IP: net.ParseIP(host)}); err != nil {
						t.Fatalf("migration: %v", err)
					}
					if path.ID() != 1 || path.LocalAddr().String() == before {
						t.Fatal("migration did not retain path ID and change local address")
					}
				}
				for migration := 0; migration < 2; migration++ {
					migrate()
					fetch("large.bin", largePayload, nil)
				}
				fetch("large.bin", largePayload, migrate)
				if grease {
					checkBitCounts(t, "independent client", &relay.clientBits, true)
					checkBitCounts(t, "Picoquic server", &relay.serverBits, true)
				}
				wantDrops := int32(0)
				if loss {
					wantDrops = 4 // OpenPath plus three migrations, in each direction.
				}
				if relay.requestDrops.Load() != wantDrops || relay.responseDrops.Load() != wantDrops {
					t.Fatalf("drops request=%d response=%d want=%d", relay.requestDrops.Load(), relay.responseDrops.Load(), wantDrops)
				}
			})
		}
	}
}

// Reverse-role coverage uses our application driver with the unmodified peer
// library. The optional demo diagnostic retains the original readiness failure.
func TestPicoquicClientMultipath(t *testing.T) {
	testPicoquicClientMultipath(t, false, false)
}

func TestPicoquicClientGreasedMultipath(t *testing.T) {
	testPicoquicClientMultipath(t, true, false)
}

func TestPicoquicClientCompatibleMultipath(t *testing.T) {
	testPicoquicClientMultipath(t, true, true)
}

func testPicoquicClientMultipath(t *testing.T, grease, compatible bool) {
	for _, host := range []string{"127.0.0.1", "::1"} {
		t.Run(host, func(t *testing.T) { testPicoquicClientMultipathHost(t, grease, compatible, host) })
	}
}

func testPicoquicClientMultipathHost(t *testing.T, grease, compatible bool, host string) {
	binary := os.Getenv("PICOQUIC_CLIENT")
	diagnostic := os.Getenv("PICOQUIC_REVERSE_DIAGNOSTIC") == "1"
	if diagnostic && (grease || compatible) {
		t.Skip("greasing requires the client driver, not the diagnostic demo")
	}
	if diagnostic {
		binary = os.Getenv("PICOQUIC_DEMO")
	}
	if binary == "" {
		t.Skip("set PICOQUIC_CLIENT to the pinned-library client driver; see picoquic/README.md")
	}
	for _, version := range []independent.Version{independent.Version1, independent.Version2} {
		if compatible && version == independent.Version2 && os.Getenv("QUIC_INTEROP_KNOWN_FAILURES") != "1" {
			t.Run("6b3343cf", func(t *testing.T) {
				t.Skip("pinned Picoquic lacks v2-to-v1 compatible switching; see KNOWN_FAILURES.md")
			})
			continue
		}
		for _, impairment := range []struct {
			name              string
			request, response bool
			bulk              bool
		}{{"clean", false, false, false}, {"request", true, false, false}, {"response", false, true, false}, {"both", true, true, false}, {"bulk", false, false, true}} {
			if diagnostic && impairment.name != "clean" {
				continue
			}
			t.Run(fmt.Sprintf("%x/%s", uint32(version), impairment.name), func(t *testing.T) {
				_, serverTLS := tlsConfigs(t)
				serverTLS.NextProtos = []string{"hq-interop"}
				selected := version
				versions := []independent.Version{version}
				if compatible {
					selected = independent.Version1
					if selected == version {
						selected = independent.Version2
					}
					versions = []independent.Version{selected, version}
				}
				listener, err := independent.ListenAddr(net.JoinHostPort(host, "0"), serverTLS, &independent.Config{EnableCompatibleVersionNegotiation: compatible, EnableMultipath: true, EnableQUICBitGreasing: grease, Versions: versions})
				if err != nil {
					t.Fatal(err)
				}
				defer listener.Close()
				relay := newMigrationRelay(t, listener.Addr().(*net.UDPAddr), impairment.request, impairment.response)
				defer relay.close()
				dir := t.TempDir()
				certPath := filepath.Join(dir, "root.pem")
				if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverTLS.Certificates[0].Certificate[0]}), 0600); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				logPath := filepath.Join(dir, "client.log")
				log, err := os.Create(logPath)
				if err != nil {
					t.Fatal(err)
				}
				port := strconv.Itoa(relay.front.LocalAddr().(*net.UDPAddr).Port)
				versionText := fmt.Sprintf("%x", uint32(version))
				args := []string{certPath, port, versionText, filepath.Join(dir, "payload.bin")}
				if compatible {
					args = append(args, fmt.Sprintf("--desired-version=%x", uint32(selected)))
				}
				if host == "::1" {
					args = append(args, "--ipv6")
				}
				if grease {
					args = append(args, "--grease-quic-bit")
				}
				if diagnostic {
					alt := "0.0.0.0/0"
					if host == "::1" {
						alt = "::/0"
					}
					args = []string{"-q", dir, "-M", "-A", alt, "-a", "hq-interop", "-n", "localhost", "-t", certPath, "-v", versionText, "-o", dir, host, port, "payload.bin"}
				}
				cmd := exec.CommandContext(ctx, binary, args...)
				cmd.Dir = dir
				cmd.Stdout, cmd.Stderr = log, log
				if err := cmd.Start(); err != nil {
					log.Close()
					t.Fatal(err)
				}
				done := make(chan struct{})
				var processErr error
				go func() { processErr = cmd.Wait(); close(done) }()
				defer func() {
					cmd.Process.Kill()
					<-done
					log.Close()
					if t.Failed() {
						data, _ := os.ReadFile(logPath)
						t.Logf("picoquic client: %s", data)
						logs, _ := filepath.Glob(filepath.Join(dir, "*.qlog"))
						for _, name := range logs {
							data, _ := os.ReadFile(name)
							t.Logf("qlog: %s", data)
						}
					}
				}()
				server, err := listener.Accept(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if compatible && relay.versionNegotiations.Load() != 0 {
					t.Fatal("incompatible negotiation used instead of switching")
				}
				if server.ConnectionState().Version != selected {
					t.Fatalf("negotiated version=%x want=%x", server.ConnectionState().Version, selected)
				}
				if !server.ConnectionState().SupportsMultipath {
					t.Fatal("multipath not negotiated")
				}
				stream, err := server.AcceptStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				deadline, _ := ctx.Deadline()
				if err := stream.SetDeadline(deadline); err != nil {
					t.Fatal(err)
				}
				request, err := io.ReadAll(io.LimitReader(stream, 1024))
				if err != nil || string(request) != "GET /payload.bin\r\n" {
					t.Fatalf("request %q: %v", request, err)
				}
				payload := make([]byte, 1<<20)
				for i := range payload {
					payload[i] = byte(i*31 + i/251)
				}
				if _, err := stream.Write(payload[:1024]); err != nil {
					t.Fatal(err)
				}
				for {
					if err = server.AbandonPath(ctx, 0); err == nil {
						break
					}
					select {
					case <-done:
						t.Fatalf("peer exited before additional-path validation: %v (last abandonment error: %v)", processErr, err)
					case <-ctx.Done():
						t.Fatalf("waiting for validated additional path: %v", err)
					case <-time.After(10 * time.Millisecond):
					}
				}
				local, remote, ok := server.PathAddresses(1)
				if !ok || local.String() != listener.Addr().String() || remote.String() == server.RemoteAddr().String() {
					t.Fatalf("additional path binding: local=%v remote=%v found=%t", local, remote, ok)
				}
				if impairment.bulk {
					relay.dropLargeResponses(remote.String(), 3)
				}
				if n, err := stream.Write(payload[1024:]); err != nil || n != len(payload)-1024 {
					t.Fatalf("write %d: %v", n, err)
				}
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
				<-done
				if processErr != nil {
					t.Fatal(processErr)
				}
				received, err := os.ReadFile(filepath.Join(dir, "payload.bin"))
				if err != nil || !bytes.Equal(received, payload) {
					t.Fatalf("download len=%d want=%d: %v", len(received), len(payload), err)
				}
				if grease {
					// ACK frequency controls the client packet count for this download.
					set, clear := relay.clientBits.set.Load(), relay.clientBits.clear.Load()
					if set == 0 || clear == 0 {
						t.Fatalf("Picoquic client bit never varied: set=%d clear=%d", set, clear)
					}
					t.Logf("Picoquic client set=%d clear=%d", set, clear)
					checkBitCounts(t, "independent server", &relay.serverBits, true)
				}
				var wantRequest, wantResponse int32
				if impairment.request {
					wantRequest = 1
				}
				if impairment.response {
					wantResponse = 1
				}
				if relay.requestDrops.Load() != wantRequest || relay.responseDrops.Load() != wantResponse {
					t.Fatalf("drops request=%d response=%d want=%d/%d", relay.requestDrops.Load(), relay.responseDrops.Load(), wantRequest, wantResponse)
				}
				wantBulk := int32(0)
				if impairment.bulk {
					wantBulk = 3
				}
				if relay.largeResponseDrops.Load() != wantBulk {
					t.Fatalf("large response drops=%d want=%d", relay.largeResponseDrops.Load(), wantBulk)
				}
				relay.mu.Lock()
				bindings := len(relay.backs)
				relay.mu.Unlock()
				if bindings != 2 {
					t.Fatalf("NAT bindings=%d want=2", bindings)
				}

			})
		}
	}
}
