package main

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"net"
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

// freePort finds a base port with the three following UDP ports free.
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
		if stop, err = serve(context.Background(), "127.0.0.1", port, cert, &out); err == nil {
			break
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	mix, err := parseFlows("photon-bbr:1,photon-newreno:1,quic-go:1")
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
