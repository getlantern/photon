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
