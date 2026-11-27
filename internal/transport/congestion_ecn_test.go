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
			useNewReno(t, c)
			c.recovery.EnableECN()
			at := time.Unix(100, 0)
			send := func(pn uint64, sent time.Time) {
				t.Helper()
				if err := c.recovery.OnSent(recovery.Initial, recovery.Packet{Number: pn, SentAt: sent, Bytes: 1200, InFlight: true, AckEliciting: true, ECN: recovery.ECT0}); err != nil {
					t.Fatal(err)
				}
			}
			// Feedback reporting the first packet still ECT validates the path.
			send(0, at)
			if err := c.handleAck(initialSpace, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}, ECN: &[3]uint64{1, 0, 0}}, at.Add(time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			send(1, at.Add(2*time.Millisecond))
			frame := wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 1, Largest: 1}}, ECN: &[3]uint64{1, 0, 1}}
			if bad {
				frame.ECN = &[3]uint64{1, 0, 2}
			}
			if err := c.handleAck(initialSpace, frame, at.Add(3*time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			iw := recovery.InitialWindow(1200)
			want := (iw + 1200) / 2
			if bad {
				want = iw + 2400 // Valid packet ACKs still grow the window despite invalid ECN.
			}
			if got := c.congestion.Window(); got != want {
				t.Fatalf("v=%v bad=%v window=%d want=%d", v, bad, got, want)
			}
			if err := c.handleAck(initialSpace, frame, at.Add(4*time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			if c.congestion.Window() != want {
				t.Fatal("duplicate changed congestion")
			}
		}
	}
}

// CE marks before the path has passed ECN validation don't start a
// recovery period: a path remarking every probe CE fails validation first.
func TestEngineECNCongestionNeedsValidation(t *testing.T) {
	c, _ := testPair(t, wire.Version1)
	useNewReno(t, c)
	c.recovery.EnableECN()
	at := time.Unix(100, 0)
	if err := c.recovery.OnSent(recovery.Initial, recovery.Packet{Number: 0, SentAt: at, Bytes: 1200, InFlight: true, AckEliciting: true, ECN: recovery.ECT0}); err != nil {
		t.Fatal(err)
	}
	if err := c.handleAck(initialSpace, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}, ECN: &[3]uint64{0, 0, 1}}, at.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if got, want := c.congestion.Window(), recovery.InitialWindow(1200)+1200; got != want {
		t.Fatalf("window %d after CE on an unvalidated path, want %d", got, want)
	}
}

func TestMultipathECNCongestion(t *testing.T) {
	p, a, b := pathRecoveryPair(t)
	useNewRenoPaths(t, p)
	b.sent.EnableECN()
	at := time.Unix(100, 0)
	send := func(pn uint64, sent time.Time) {
		t.Helper()
		if err := b.recordSent(recovery.Packet{Number: pn, SentAt: sent, Bytes: 1200, InFlight: true, AckEliciting: true, ECN: recovery.ECT0}); err != nil {
			t.Fatal(err)
		}
	}
	ackFrame := func(pn uint64, counts [3]uint64) wire.PathAckFrame {
		return wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: pn, Largest: pn}}, ECN: &counts}}
	}
	send(0, at)
	if _, err := p.receiveACK(ackFrame(0, [3]uint64{1, 0, 0}), at.Add(time.Millisecond), 3, 25*time.Millisecond, true); err != nil {
		t.Fatal(err)
	}
	send(1, at.Add(2*time.Millisecond))
	frame := ackFrame(1, [3]uint64{1, 0, 1})
	if _, err := p.receiveACK(frame, at.Add(3*time.Millisecond), 3, 25*time.Millisecond, true); err != nil {
		t.Fatal(err)
	}
	iw := recovery.InitialWindow(1200)
	if a.congestion.Window() != iw || b.congestion.Window() != iw/2 {
		t.Fatal("CE affected wrong path", a.congestion.Window(), b.congestion.Window())
	}
	if _, err := p.receiveACK(frame, at.Add(4*time.Millisecond), 3, 25*time.Millisecond, true); err != nil {
		t.Fatal(err)
	}
	if b.congestion.Window() != iw/2 {
		t.Fatal("duplicate CE reduced twice")
	}
}
