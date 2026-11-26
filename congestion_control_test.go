package quic

import (
	"bytes"
	"context"
	"io"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/transport"
)

// The zero value selects BBR, and each controller maps to its own.
func TestCongestionControlDefaultIsBBR(t *testing.T) {
	for _, c := range []struct {
		config CongestionControl
		want   recovery.Algorithm
	}{{0, recovery.BBRAlgorithm}, {CongestionControlBBR, recovery.BBRAlgorithm}, {CongestionControlNewReno, recovery.NewRenoAlgorithm}} {
		cfg, _, err := prepareConfig(&Config{CongestionControl: c.config})
		if err != nil {
			t.Fatal(err)
		}
		if got := cfg.congestionAlgorithm(); got != c.want {
			t.Fatalf("CongestionControl %d selected %d, want %d", c.config, got, c.want)
		}
	}
}

// Two endpoints configured for NewReno complete a transfer, as two with the
// default BBR do throughout the package's other tests: the option reaches
// both the dialing and the listening side.
func TestCongestionControlNewRenoTransfer(t *testing.T) {
	ct, st := dialTLS(t)
	config := &Config{CongestionControl: CongestionControlNewReno}
	l, err := ListenAddr("127.0.0.1:0", st, config)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := DialAddr(ctx, l.Addr().String(), ct, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseWithError(0, "")
	server, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer server.CloseWithError(0, "")
	payload := bytes.Repeat([]byte("reno"), 150_000)
	stream, err := client.OpenUniStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		stream.Write(payload)
		stream.Close()
	}()
	in, err := server.AcceptUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	in.SetReadDeadline(time.Now().Add(10 * time.Second))
	got, err := io.ReadAll(in)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatal("transfer", len(got), err)
	}
}

// An unknown CongestionControl is rejected rather than silently run as
// another controller.
func TestCongestionControlUnknownRejected(t *testing.T) {
	_, st := dialTLS(t)
	if _, err := ListenAddr("127.0.0.1:0", st, &Config{CongestionControl: CongestionControlNewReno + 1}); err == nil {
		t.Fatal("unknown congestion control accepted")
	}
}

// A config chosen by GetConfigForClient selects that connection's controller,
// whatever the listener's.
func TestCongestionControlFromGetConfigForClient(t *testing.T) {
	for _, c := range []struct {
		selected CongestionControl
		want     recovery.Algorithm
	}{{CongestionControlNewReno, recovery.NewRenoAlgorithm}, {CongestionControlBBR, recovery.BBRAlgorithm}} {
		selectConfig := serverConfigSelector(Config{GetConfigForClient: func(*ClientInfo) (*Config, error) {
			return &Config{CongestionControl: c.selected}, nil
		}})
		got, err := selectConfig(transport.Config{CongestionControl: recovery.BBRAlgorithm}, netip.MustParseAddrPort("127.0.0.1:1"))
		if err != nil {
			t.Fatal(err)
		}
		if got.CongestionControl != c.want {
			t.Fatalf("selected %d, got %d, want %d", c.selected, got.CongestionControl, c.want)
		}
	}
}
