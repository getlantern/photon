package quic

import (
	"github.com/getlantern/photon/internal/wire"
	"testing"
)

func TestReceiveWindowConfiguration(t *testing.T) {
	for _, window := range []uint64{0, 1, 65537, 128 << 10, 16 << 20} {
		input := Config{InitialStreamReceiveWindow: window, InitialConnectionReceiveWindow: window}
		cfg, p, err := prepareConfig(&input)
		if err != nil {
			t.Fatal(window, err)
		}
		streamWindow, connectionWindow := window, window
		if window == 0 {
			streamWindow, connectionWindow = 512<<10, 768<<10
		}
		if p.InitialMaxData != connectionWindow || p.InitialMaxStreamDataBidiLocal != streamWindow || p.InitialMaxStreamDataBidiRemote != streamWindow || p.InitialMaxStreamDataUni != streamWindow || cfg.InitialStreamReceiveWindow != streamWindow {
			t.Fatal("incorrect receive policy", cfg, p)
		}
		if cfg.MaxStreamReceiveWindow != max(6<<20, streamWindow) || cfg.MaxConnectionReceiveWindow != max(15<<20, connectionWindow) {
			t.Fatal("incorrect default growth limits", cfg)
		}
		if input.InitialStreamReceiveWindow != window || input.InitialConnectionReceiveWindow != window {
			t.Fatal("caller config mutated")
		}
	}
	for _, input := range []Config{{InitialStreamReceiveWindow: 16<<20 + 1}, {InitialConnectionReceiveWindow: wire.MaxVarint + 1}, {MaxStreamReceiveWindow: 1}, {MaxConnectionReceiveWindow: 1}, {MaxStreamReceiveWindow: 16<<20 + 1}, {MaxConnectionReceiveWindow: wire.MaxVarint + 1}} {
		if _, _, err := prepareConfig(&input); err == nil {
			t.Fatal("unsupported receive policy accepted")
		}
	}
}

func TestReceiveWindowGrowthConfiguration(t *testing.T) {
	input := Config{InitialStreamReceiveWindow: 1024, InitialConnectionReceiveWindow: 2048, MaxStreamReceiveWindow: 4096, MaxConnectionReceiveWindow: 8192}
	cfg, p, err := prepareConfig(&input)
	if err != nil || cfg.MaxStreamReceiveWindow != 4096 || cfg.MaxConnectionReceiveWindow != 8192 || p.InitialMaxData != 2048 || p.InitialMaxStreamDataBidiRemote != 1024 {
		t.Fatal("growth policy changed initial credit", cfg, p, err)
	}
}
