package recovery

import (
	"testing"
	"time"
)

func TestECNAndLossShareRecovery(t *testing.T) {
	start := time.Unix(100, 0)
	for _, ecnFirst := range []bool{true, false} {
		c := congestion(t)
		iw := InitialWindow(1200)
		p := Packet{SentAt: start, Bytes: 1200, InFlight: true}
		if ecnFirst {
			c.OnECN(p.SentAt, start.Add(time.Second))
		} else {
			c.OnLoss(LossEvent{Lost: []Packet{p}, Now: start.Add(time.Second)})
		}
		if c.Window() != iw/2 || c.Threshold() != iw/2 {
			t.Fatal("initial reduction", c.Window())
		}
		if ecnFirst {
			c.OnLoss(LossEvent{Lost: []Packet{p}, Now: start.Add(2 * time.Second)})
		} else {
			c.OnECN(p.SentAt, start.Add(2*time.Second))
		}
		// A larger CE total might acknowledge another pre-recovery packet. It must
		// not begin a fresh recovery epoch merely because its ACK arrived later.
		c.OnECN(start.Add(time.Second), start.Add(3*time.Second))
		c.OnACK(ACKEvent{Acked: []Packet{p}, Limited: false})
		if c.Window() != iw/2 {
			t.Fatal("same epoch reduced or grew twice", c.Window())
		}
		c.OnECN(start.Add(2*time.Second), start.Add(4*time.Second))
		if c.Window() != iw/4 {
			t.Fatal("new epoch not reduced", c.Window())
		}
		now := start.Add(4 * time.Second)
		for c.Window()/2 >= 2400 {
			want := c.Window() / 2
			c.OnECN(now.Add(time.Second), now.Add(2*time.Second))
			now = now.Add(2 * time.Second)
			if c.Window() != want {
				t.Fatal("new epoch not reduced", c.Window(), want)
			}
		}
		threshold := c.Window() / 2
		c.OnECN(now.Add(time.Second), now.Add(2*time.Second))
		if c.Window() != 2400 || c.Threshold() != threshold {
			t.Fatal("minimum congestion window", c.Window(), c.Threshold())
		}
	}
}

func TestECNResetsGrowthCreditAndIsPathLocal(t *testing.T) {
	start := time.Unix(100, 0)
	c := congestion(t)
	other := congestion(t)
	iw := InitialWindow(1200)
	c.OnECN(start, start.Add(time.Second))
	c.OnACK(ACKEvent{Acked: []Packet{{SentAt: start.Add(2 * time.Second), Bytes: int(iw/2 - 1), InFlight: true}}, Limited: false})
	if c.Window() != iw/2 {
		t.Fatal("premature additive increase")
	}
	c.OnECN(start.Add(2*time.Second), start.Add(3*time.Second))
	c.OnACK(ACKEvent{Acked: []Packet{{SentAt: start.Add(4 * time.Second), Bytes: 1, InFlight: true}}, Limited: false})
	if c.Window() != iw/4 {
		t.Fatal("old credit survived ECN reduction")
	}
	if other.Window() != iw {
		t.Fatal("another path changed")
	}
	// Application-limited ACK growth does not suppress a congestion signal.
	c.OnACK(ACKEvent{Acked: []Packet{{SentAt: start.Add(4 * time.Second), Bytes: 1200, InFlight: true}}, Limited: true})
	c.OnECN(start.Add(4*time.Second), start.Add(5*time.Second))
	if c.Window() != max(iw/8, 2400) {
		t.Fatal("application-limited CE ignored")
	}
}
