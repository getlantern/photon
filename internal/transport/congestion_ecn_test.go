package transport

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func TestEngineECNCongestion(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, bad := range []bool{false, true} {
			c, _ := testPair(t, v)
			c.congestion = renoController(t)
			at := time.Unix(100, 0)
			if err := c.recovery.OnSent(recovery.Initial, recovery.Packet{Number: 0, SentAt: at, Bytes: 1200, InFlight: true, AckEliciting: true, ECN: recovery.ECT0}); err != nil {
				t.Fatal(err)
			}
			frame := wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}, ECN: &[3]uint64{0, 0, 1}}
			if bad {
				frame.ECN = &[3]uint64{0, 0, 2}
			}
			if err := c.handleAck(initialSpace, frame, at.Add(time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			want := uint64(6000)
			if bad {
				want = 13200 // Valid packet ACK still grows the window despite invalid ECN.
			}
			if got := c.congestion.Window(); got != want {
				t.Fatalf("v=%v bad=%v window=%d want=%d", v, bad, got, want)
			}
			if err := c.handleAck(initialSpace, frame, at.Add(2*time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			if c.congestion.Window() != want {
				t.Fatal("duplicate changed congestion")
			}
		}
	}
}

func TestMultipathECNCongestion(t *testing.T) {
	p, a, b := pathRecoveryPair(t)
	a.congestion, b.congestion = renoController(t), renoController(t)
	at := time.Unix(100, 0)
	if err := b.recordSent(recovery.Packet{Number: 0, SentAt: at, Bytes: 1200, InFlight: true, AckEliciting: true, ECN: recovery.ECT0}); err != nil {
		t.Fatal(err)
	}
	frame := wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}, ECN: &[3]uint64{0, 0, 1}}}
	if _, err := p.receiveACK(frame, at.Add(time.Millisecond), 3, 25*time.Millisecond, true); err != nil {
		t.Fatal(err)
	}
	if a.congestion.Window() != 12000 || b.congestion.Window() != 6000 {
		t.Fatal("CE affected wrong path", a.congestion.Window(), b.congestion.Window())
	}
	if _, err := p.receiveACK(frame, at.Add(2*time.Millisecond), 3, 25*time.Millisecond, true); err != nil {
		t.Fatal(err)
	}
	if b.congestion.Window() != 6000 {
		t.Fatal("duplicate CE reduced twice")
	}
}
