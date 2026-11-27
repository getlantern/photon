package main

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// lockedBuffer collects the server's statistics lines across goroutines.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// freePort finds a base port with one UDP port free for each sender.
func freePort(t *testing.T) int {
	t.Helper()
	for range 20 {
		c, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := c.LocalAddr().(*net.UDPAddr).Port
		c.Close()
		ok := true
		for i := range senders {
			c, err := net.ListenPacket("udp", net.JoinHostPort("127.0.0.1", itoa(port+i)))
			if err != nil {
				ok = false
				break
			}
			c.Close()
		}
		if ok {
			return port
		}
	}
	t.Fatal("no free ports")
	return 0
}

func itoa(n int) string { return strconv.Itoa(n) }

// A short localhost run of all three senders: every flow moves data and
// every connection logs its statistics.
func TestPathbenchLocalhost(t *testing.T) {
	cert, err := selfSigned()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(parsed)
	var out lockedBuffer
	var port int
	var stop func()
	// Another process can take a port between freePort and serve; retry.
	for range 5 {
		port = freePort(t)
		if stop, _, err = serve(context.Background(), "127.0.0.1", port, cert, &out); err == nil {
			break
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	mix, err := parseFlows("photon-bbr:1,photon-newreno:1,quic-go:1,hysteria-bbr:1")
	if err != nil {
		t.Fatal(err)
	}
	res, err := runClient(context.Background(), clientConfig{server: "127.0.0.1", port: port, mix: mix, duration: 3 * time.Second, from: time.Second, stagger: 200 * time.Millisecond, handshakeTimeout: 5 * time.Second, roots: roots})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range res.Flows {
		if f.Error != "" || f.Mbps <= 0 {
			t.Fatalf("flow %d (%s): %.1f Mbit/s, error %q", f.Flow, f.Sender, f.Mbps, f.Error)
		}
	}
	// Each connection logs its statistics once it closes.
	deadline := time.Now().Add(5 * time.Second)
	for strings.Count(out.String(), "\n") < len(mix) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	stop()
	seen := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		var s connStats
		if err := json.Unmarshal([]byte(line), &s); err != nil {
			t.Fatal(line, err)
		}
		if s.BytesSent == 0 {
			t.Fatalf("%s logged no bytes sent: %s", s.Sender, line)
		}
		seen[s.Sender] = true
	}
	for _, s := range senders {
		if !seen[s] {
			t.Fatalf("no statistics from %s in %q", s, out.String())
		}
	}
}

func TestParseFlows(t *testing.T) {
	mix, err := parseFlows("photon-bbr:2, quic-go")
	if err != nil || strings.Join(mix, ",") != "photon-bbr,photon-bbr,quic-go" {
		t.Fatal(mix, err)
	}
	for _, bad := range []string{"cubic:1", "photon-bbr:0", "photon-bbr:x"} {
		if _, err := parseFlows(bad); err == nil {
			t.Fatal("accepted", bad)
		}
	}
}

func TestRunClientRejectsBadTiming(t *testing.T) {
	for _, cfg := range []clientConfig{
		{mix: []string{"photon-bbr"}, duration: 10 * time.Second, from: 9 * time.Second},
		{mix: []string{"photon-bbr", "quic-go"}, duration: 10 * time.Second, from: 2 * time.Second, stagger: -time.Second},
		{mix: []string{"photon-bbr", "quic-go"}, duration: 10 * time.Second, from: time.Second, stagger: 2 * time.Second},
	} {
		if _, err := runClient(context.Background(), cfg); err == nil {
			t.Fatalf("accepted %+v", cfg)
		}
	}
}

// A failed flow still counts toward the total and the shares: it used the
// path while it ran, and leaving it out would hand its share to the others.
func TestSummarizeCountsFailedFlows(t *testing.T) {
	res := summarize("h", 10, []flowSample{
		{sender: "photon-bbr", bytes: 80e6 / 8 * 10},
		{sender: "quic-go", bytes: 20e6 / 8 * 10, err: "read: connection lost"},
	})
	ok, failed := res.Flows[0], res.Flows[1]
	if failed.Error == "" || failed.Mbps != 20 || res.TotalMbps != 100 || ok.SharePercent != 80 || failed.SharePercent != 20 {
		t.Fatalf("flows %+v, total %.1f: want the failed flow's 20 Mbit/s counted", res.Flows, res.TotalMbps)
	}
}

// A flow that never connects is reported failed, though it moved nothing.
// The client's base port is one past the server's: its photon-bbr flow
// reaches the server's photon-newreno listener, and its hysteria-bbr flow,
// the last sender, dials the port after the server's last, where none
// listens.
func TestPathbenchUnreachableFlowFails(t *testing.T) {
	cert, err := selfSigned()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(parsed)
	var out lockedBuffer
	var port int
	var stop func()
	for range 5 {
		port = freePort(t)
		c, e := net.ListenPacket("udp", net.JoinHostPort("127.0.0.1", itoa(port+len(senders))))
		if e != nil {
			continue
		}
		c.Close()
		if stop, _, err = serve(context.Background(), "127.0.0.1", port, cert, &out); err == nil {
			break
		}
	}
	if stop == nil {
		t.Fatal("no server started", err)
	}
	t.Cleanup(stop)
	res, err := runClient(context.Background(), clientConfig{server: "127.0.0.1", port: port + 1, mix: []string{"photon-bbr", "hysteria-bbr"}, duration: 3 * time.Second, from: time.Second, stagger: 200 * time.Millisecond, handshakeTimeout: 5 * time.Second, roots: roots})
	if err != nil {
		t.Fatal(err)
	}
	if ok, failed := res.Flows[0], res.Flows[1]; ok.Error != "" || ok.Mbps <= 0 || failed.Error == "" {
		t.Fatalf("flows %+v, want the first to run and the second to fail", res.Flows)
	}
}

// A flow whose first data arrives after the measurement window opens didn't
// compete for the whole window, so it is reported failed.
func TestPathbenchLateFirstDataFails(t *testing.T) {
	responseDelay = 1500 * time.Millisecond
	t.Cleanup(func() { responseDelay = 0 })
	cert, err := selfSigned()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(parsed)
	var out lockedBuffer
	var stop func()
	var port int
	for range 5 {
		port = freePort(t)
		if stop, _, err = serve(context.Background(), "127.0.0.1", port, cert, &out); err == nil {
			break
		}
	}
	if stop == nil {
		t.Fatal("no server started", err)
	}
	t.Cleanup(stop)
	res, err := runClient(context.Background(), clientConfig{server: "127.0.0.1", port: port, mix: []string{"photon-bbr"}, duration: 3 * time.Second, from: time.Second, handshakeTimeout: 5 * time.Second, roots: roots})
	if err != nil {
		t.Fatal(err)
	}
	if f := res.Flows[0]; f.Error == "" || f.Mbps <= 0 {
		t.Fatalf("flow %+v, want data after the window opened, reported failed", f)
	}
}

// A second server started on a running one's ports fails without replacing
// the certificate the running server's clients use.
func TestServerKeepsRunningServersCert(t *testing.T) {
	cert, err := selfSigned()
	if err != nil {
		t.Fatal(err)
	}
	var port int
	var stop func()
	for range 5 {
		port = freePort(t)
		if stop, _, err = serve(context.Background(), "127.0.0.1", port, cert, io.Discard); err == nil {
			break
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	certFile := filepath.Join(t.TempDir(), "cert.pem")
	want := []byte("running server's certificate")
	if err := os.WriteFile(certFile, want, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := serverMain([]string{"-port", strconv.Itoa(port), "-cert-out", certFile}); err == nil {
		t.Fatal("second server started on busy ports")
	}
	if got, err := os.ReadFile(certFile); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("certificate file replaced: %q, %v", got, err)
	}
}
