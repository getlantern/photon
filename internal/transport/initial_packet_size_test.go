package transport

import (
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func TestConfiguredInitialPacketSize(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, size := range []uint16{1200, 1280, 1452} {
			t.Run(fmt.Sprintf("%x/%d", version, size), func(t *testing.T) {
				c, s := testPair(t, version)
				c.config.InitialPacketSize = size
				s.config.InitialPacketSize = size
				now := time.Unix(100, 0)
				for _, pair := range [][2]*Conn{{c, s}, {s, c}} {
					packets := drain(t, pair[0], now)
					initials := 0
					for _, b := range packets {
						h, err := wire.ParseHeader(b, 8)
						if err != nil {
							t.Fatal(err)
						}
						if h.Type == wire.Initial {
							initials++
							if len(b) != int(size) {
								t.Fatalf("Initial size %d, want %d", len(b), size)
							}
						}
						if err := pair[1].HandleDatagram(b, now); err != nil {
							t.Fatal(err)
						}
					}
					if initials == 0 {
						t.Fatal("no Initial emitted")
					}
				}
				drivePair(t, c, s, false, false)
				if !c.confirmed || !s.confirmed {
					t.Fatal("sized Initial broke handshake")
				}
			})
		}
	}
}

func TestInitialPacketSizePeerLimit(t *testing.T) {
	c, _ := testPair(t, wire.Version1)
	c.config.InitialPacketSize = 1452
	p := wire.DefaultTransportParameters()
	p.MaxUDPPayloadSize = 1280
	c.peerParams = &p
	packets := drain(t, c, time.Unix(100, 0))
	if len(packets) == 0 {
		t.Fatal("no Initial")
	}
	for _, b := range packets {
		if len(b) != 1280 {
			t.Fatal("peer UDP limit ignored", len(b))
		}
	}
}

func TestInitialPacketSizeValidation(t *testing.T) {
	c, _ := testPair(t, wire.Version1)
	for _, size := range []uint16{1, 1199, 1453, 65535} {
		cfg := c.config
		cfg.InitialPacketSize = size
		if bad, err := NewConn(cfg); err == nil {
			bad.Close()
			t.Fatal("invalid initial size accepted", size)
		}
	}
	for _, size := range []uint16{0, 1200, 1280, 1452} {
		cfg := c.config
		cfg.InitialPacketSize = size
		good, err := NewConn(cfg)
		if err != nil {
			t.Fatal(err)
		}
		want := size
		if want == 0 {
			want = 1200
		}
		if good.config.InitialPacketSize != want || good.congestion.Window() != recovery.InitialWindow(uint64(want)) {
			t.Fatal("initial packet/congestion size", size, good.congestion.Window())
		}
		good.Close()
	}
}

func TestOversizedInitialFrameIsNotTruncated(t *testing.T) {
	c, _ := testPair(t, wire.Version1)
	c.spaces[initialSpace].queue = nil
	if err := c.enqueue(initialSpace, wire.CryptoFrame{Data: make([]byte, 1500)}, 1500); err != nil {
		t.Fatal(err)
	}
	before := c.spaces[initialSpace].nextPN
	packet, err := c.NextDatagram(time.Unix(100, 0))
	if err == nil || len(packet) != 0 {
		t.Fatal("oversized CRYPTO was emitted", len(packet), err)
	}
	if c.spaces[initialSpace].nextPN != before || c.sealed[initialSpace] != 0 {
		t.Fatal("invalid frame committed a send")
	}
}

func TestConfiguredInitialSizeSurvivesRetry(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		c, _ := testPair(t, version)
		c.config.InitialPacketSize = 1452
		now := time.Unix(100, 0)
		drain(t, c, now)
		// This token cannot leave CRYPTO space in the old fixed 1200 budget.
		token := make([]byte, 1200)
		if err := c.HandleDatagram(retryPacket(t, c, []byte("retrycid"), token), now.Add(time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		if c.congestion.Window() != recovery.InitialWindow(1452) {
			t.Fatal("Retry lost configured congestion size", c.congestion.Window())
		}
		packets := drain(t, c, now.Add(2*time.Millisecond))
		if len(packets) == 0 {
			t.Fatal("Retry produced no Initial")
		}
		for _, b := range packets {
			h, err := wire.ParseHeader(b, 8)
			if err != nil || h.Type != wire.Initial || len(b) != 1452 || len(h.Token) != len(token) {
				t.Fatal("Retry repacketization size", len(b), err)
			}
		}
	}
}
