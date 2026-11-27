package recovery

import (
	"testing"
	"time"
)

func congestion(t *testing.T) *NewReno {
	t.Helper()
	c, err := NewCongestion(1200)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func TestInitialWindowAndSendGate(t *testing.T) {
	for _, tc := range []struct{ mss, want uint64 }{{1200, 32 * 1200}, {1500, 32 * 1500}, {9000, 32 * 9000}} {
		c, err := NewCongestion(tc.mss)
		if err != nil || c.Window() != tc.want {
			t.Fatal(tc, c, err)
		}
	}
	c := congestion(t)
	iw := InitialWindow(1200)
	if !c.CanSend(iw-1200, 1200, true, false) || c.CanSend(iw-1199, 1200, true, false) || c.CanSend(^uint64(0), 1200, true, false) {
		t.Fatal("window boundary")
	}
	if !c.CanSend(iw, 100, false, false) || !c.CanSend(iw, 1200, true, true) || c.CanSend(0, -1, false, true) {
		t.Fatal("ACK/probe exception")
	}
}
func TestSlowStartAndRecovery(t *testing.T) {
	c := congestion(t)
	iw := InitialWindow(1200)
	start := time.Unix(1000, 0)
	p := Packet{SentAt: start, Bytes: 1200, InFlight: true, AckEliciting: true}
	c.OnACK(ACKEvent{Acked: []Packet{p}, Limited: true})
	if c.Window() != iw {
		t.Fatal("application limited growth")
	}
	c.OnACK(ACKEvent{Acked: []Packet{p}, Limited: false})
	if c.Window() != iw+1200 {
		t.Fatal("slow start")
	}
	half := (iw + 1200) / 2
	c.OnLoss(LossEvent{Lost: []Packet{p}, Now: start.Add(time.Second)})
	if c.Window() != half || c.Threshold() != half {
		t.Fatal("loss reduction")
	}
	c.OnLoss(LossEvent{Lost: []Packet{p}, Now: start.Add(2 * time.Second)})
	c.OnACK(ACKEvent{Acked: []Packet{p}, Limited: false})
	if c.Window() != half {
		t.Fatal("recovery changed window twice")
	}
	p.SentAt = start.Add(3 * time.Second)
	// A window's worth of ACKed bytes increases cwnd by one max-sized
	// datagram; tiny ACKed packets eventually grow it too, without
	// integer-division starvation.
	p.Bytes = 1
	for i := uint64(0); i < half; i++ {
		c.OnACK(ACKEvent{Acked: []Packet{p}, Limited: false})
	}
	if c.Window() != half+1200 {
		t.Fatal("congestion avoidance", c.Window())
	}
	c.OnLoss(LossEvent{Lost: []Packet{p}, Now: start.Add(4 * time.Second)})
	if c.Window() != (half+1200)/2 {
		t.Fatal("new epoch failed to reduce")
	}
	now := start.Add(4 * time.Second)
	for c.Window()/2 >= 2400 {
		want := c.Window() / 2
		p.SentAt = now.Add(time.Second)
		now = now.Add(2 * time.Second)
		c.OnLoss(LossEvent{Lost: []Packet{p}, Now: now})
		if c.Window() != want {
			t.Fatal("new epoch failed to reduce", c.Window(), want)
		}
	}
	p.SentAt = now.Add(time.Second)
	c.OnLoss(LossEvent{Lost: []Packet{p}, Now: now.Add(2 * time.Second)})
	if c.Window() != 2400 {
		t.Fatal("minimum window")
	}
}
func TestACKOnlyDoesNotAffectCongestion(t *testing.T) {
	c := congestion(t)
	p := Packet{SentAt: time.Unix(1000, 0), Bytes: 1200}
	c.OnACK(ACKEvent{Acked: []Packet{p}, Limited: false})
	c.OnLoss(LossEvent{Lost: []Packet{p}, Now: p.SentAt.Add(time.Second)})
	if c.Window() != InitialWindow(1200) {
		t.Fatal("ACK-only changed window")
	}
}

func TestPersistentCongestionEndsRecovery(t *testing.T) {
	c := congestion(t)
	now := time.Unix(1000, 0)
	p := Packet{SentAt: now, Bytes: 1200, InFlight: true, AckEliciting: true}
	c.OnLoss(LossEvent{Lost: []Packet{p}, Now: now.Add(time.Second)})
	c.credit = 100
	c.OnPersistentCongestion()
	if c.Window() != 2400 || c.recovering || c.credit != 0 {
		t.Fatal("persistent congestion state", c)
	}
	c.OnACK(ACKEvent{Acked: []Packet{p}, Limited: true})
	if c.Window() != 2400 {
		t.Fatal("application-limited growth")
	}
	c.OnACK(ACKEvent{Acked: []Packet{p}, Limited: false})
	if c.Window() != 3600 {
		t.Fatal("slow start suppressed", c.Window())
	}
}
