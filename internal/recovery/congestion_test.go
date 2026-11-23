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
	for _, tc := range []struct{ mss, want uint64 }{{1200, 12000}, {1500, 14720}, {9000, 18000}} {
		c, err := NewCongestion(tc.mss)
		if err != nil || c.Window() != tc.want {
			t.Fatal(tc, c, err)
		}
	}
	c := congestion(t)
	if !c.CanSend(10800, 1200, true, false) || c.CanSend(10801, 1200, true, false) || c.CanSend(^uint64(0), 1200, true, false) {
		t.Fatal("window boundary")
	}
	if !c.CanSend(12000, 100, false, false) || !c.CanSend(12000, 1200, true, true) || c.CanSend(0, -1, false, true) {
		t.Fatal("ACK/probe exception")
	}
}
func TestSlowStartAndRecovery(t *testing.T) {
	c := congestion(t)
	start := time.Unix(1000, 0)
	p := Packet{SentAt: start, Bytes: 1200, InFlight: true, AckEliciting: true}
	c.OnACK([]Packet{p}, true)
	if c.Window() != 12000 {
		t.Fatal("application limited growth")
	}
	c.OnACK([]Packet{p}, false)
	if c.Window() != 13200 {
		t.Fatal("slow start")
	}
	c.OnLoss([]Packet{p}, start.Add(time.Second))
	if c.Window() != 6600 || c.Threshold() != 6600 {
		t.Fatal("loss reduction")
	}
	c.OnLoss([]Packet{p}, start.Add(2*time.Second))
	c.OnACK([]Packet{p}, false)
	if c.Window() != 6600 {
		t.Fatal("recovery changed window twice")
	}
	p.SentAt = start.Add(3 * time.Second)
	// 6600 ACKed bytes increase cwnd by one max-sized datagram; tiny ACKed
	// packets eventually grow it too, without integer-division starvation.
	p.Bytes = 1
	for i := 0; i < 6600; i++ {
		c.OnACK([]Packet{p}, false)
	}
	if c.Window() != 7800 {
		t.Fatal("congestion avoidance", c.Window())
	}
	c.OnLoss([]Packet{p}, start.Add(4*time.Second))
	if c.Window() != 3900 {
		t.Fatal("new epoch failed to reduce")
	}
	p.SentAt = start.Add(5 * time.Second)
	c.OnLoss([]Packet{p}, start.Add(6*time.Second))
	if c.Window() != 2400 {
		t.Fatal("minimum window")
	}
}
func TestACKOnlyDoesNotAffectCongestion(t *testing.T) {
	c := congestion(t)
	p := Packet{SentAt: time.Unix(1000, 0), Bytes: 1200}
	c.OnACK([]Packet{p}, false)
	c.OnLoss([]Packet{p}, p.SentAt.Add(time.Second))
	if c.Window() != 12000 {
		t.Fatal("ACK-only changed window")
	}
}

func TestPersistentCongestionEndsRecovery(t *testing.T) {
	c := congestion(t)
	now := time.Unix(1000, 0)
	p := Packet{SentAt: now, Bytes: 1200, InFlight: true, AckEliciting: true}
	c.OnLoss([]Packet{p}, now.Add(time.Second))
	c.credit = 100
	c.OnPersistentCongestion()
	if c.Window() != 2400 || c.recovering || c.credit != 0 {
		t.Fatal("persistent congestion state", c)
	}
	c.OnACK([]Packet{p}, true)
	if c.Window() != 2400 {
		t.Fatal("application-limited growth")
	}
	c.OnACK([]Packet{p}, false)
	if c.Window() != 3600 {
		t.Fatal("slow start suppressed", c.Window())
	}
}
