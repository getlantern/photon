package recovery

import (
	"testing"
	"time"
)

func TestPacerBurstAndRefill(t *testing.T) {
	var p Pacer
	now := time.Unix(100, 0)
	for i := 0; i < 10; i++ {
		if p.Delay(now, 12000, 100*time.Millisecond, 1200) != 0 {
			t.Fatal("initial burst blocked", i)
		}
		p.Sent(1200)
	}
	delay := p.Delay(now, 12000, 100*time.Millisecond, 1200)
	if delay < 8*time.Millisecond || delay > 8*time.Millisecond+time.Nanosecond {
		t.Fatal("wrong pacing rate", delay)
	}
	if d := p.Delay(now.Add(delay), 12000, 100*time.Millisecond, 1200); d != 0 {
		t.Fatal("refill not available", d)
	}
	p.Sent(1200)
	if d := p.Delay(now.Add(time.Hour), 12000, 100*time.Millisecond, 12000); d != 0 {
		t.Fatal(d)
	}
	p.Sent(12000)
	if d := p.Delay(now.Add(time.Hour), 12000, 100*time.Millisecond, 1); d == 0 {
		t.Fatal("idle time accumulated unbounded burst")
	}
}
func TestPacerProbeDebtAndClockRegression(t *testing.T) {
	var p Pacer
	now := time.Unix(100, 0)
	p.Delay(now, 2400, 100*time.Millisecond, 2400)
	p.Sent(2400)
	p.Sent(1200) // PTO bypass still debits pacing credit.
	if d := p.Delay(now, 2400, 100*time.Millisecond, 1200); d < 80*time.Millisecond {
		t.Fatal("probe debt ignored", d)
	}
	if d := p.Delay(now.Add(-time.Second), 2400, 100*time.Millisecond, 1200); d < 1080*time.Millisecond {
		t.Fatal("clock regression shortened deadline", d)
	}
	if d := p.Delay(now, 0, 0, 1200); d != maxDuration {
		t.Fatal("zero window", d)
	}
}

// rateController is a controller that sets its own pacing rate.
type rateController struct {
	*NewReno
	rate uint64
}

func (c rateController) PacingRate() uint64 { return c.rate }

// A controller's own pacing rate replaces the window-derived one, with bursts
// of its send quantum; with no rate set, pacing follows the window.
func TestPacerUsesControllerRate(t *testing.T) {
	now := time.Unix(100, 0)
	reno, _ := NewCongestion(1200)
	// 1.2 MB/s: the send quantum is 1 ms, 1200 bytes, raised to two packets
	// of 1200 bytes.
	c := rateController{NewReno: reno, rate: 1_200_000}
	var p Pacer
	if d := p.DelayFor(now, c, 100*time.Millisecond, 1200); d != 0 {
		t.Fatal("first packet delayed", d)
	}
	p.Sent(1200)
	p.Sent(1200)
	p.Sent(1200)
	// The bucket held 2400 bytes; 3600 sent leaves it 1200 short, plus 1200
	// for the next packet: 2400 bytes at 1.2 MB/s.
	if d := p.DelayFor(now, c, 100*time.Millisecond, 1200); d < 2*time.Millisecond || d > 2001*time.Microsecond {
		t.Fatal("paced delay", d, "want 2ms")
	}
	var window Pacer
	c.rate = 0
	if d, want := window.DelayFor(now, c, 100*time.Millisecond, 1200), window.Delay(now, reno.Window(), 100*time.Millisecond, 1200); d != want {
		t.Fatal("rate 0 paced", d, "want the window-derived", want)
	}
}
