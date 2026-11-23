package recovery

import (
	"testing"
	"time"
)

func TestRTTEstimates(t *testing.T) {
	r := NewRTT()
	if r.PTO(false, false, 0, 0) != 999*time.Millisecond || r.LossDelay() != 374625*time.Microsecond {
		t.Fatalf("initial estimates: %+v", r)
	}
	r.Update(100*time.Millisecond, 90*time.Millisecond, 25*time.Millisecond, true)
	if r.Minimum != 100*time.Millisecond || r.Smoothed != 100*time.Millisecond || r.Variation != 50*time.Millisecond {
		t.Fatalf("first sample: %+v", r)
	}
	r.Update(140*time.Millisecond, 20*time.Millisecond, 25*time.Millisecond, true)
	if r.Minimum != 100*time.Millisecond || r.Smoothed != 102500*time.Microsecond || r.Variation != 42500*time.Microsecond {
		t.Fatalf("second sample: %+v", r)
	}
	if r.LossDelay() != 157500*time.Microsecond {
		t.Fatal(r.LossDelay())
	}
	if got := r.PTO(true, true, 25*time.Millisecond, 1); got != 595*time.Millisecond {
		t.Fatal(got)
	}
	before := r
	r.Update(-1, 0, 0, false)
	if r != before {
		t.Fatal("negative sample changed estimates")
	}
}

func TestACKDelayAdjustment(t *testing.T) {
	for _, tc := range []struct {
		name      string
		delay     time.Duration
		confirmed bool
		want      time.Duration
	}{
		{"uncapped-before-confirmation", 40 * time.Millisecond, false, 101250 * time.Microsecond},
		{"capped-after-confirmation", 40 * time.Millisecond, true, 103125 * time.Microsecond},
		{"cannot-subtract-below-minimum", 60 * time.Millisecond, false, 106250 * time.Microsecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRTT()
			r.Update(100*time.Millisecond, 0, 0, false)
			r.Update(150*time.Millisecond, tc.delay, 25*time.Millisecond, tc.confirmed)
			if r.Smoothed != tc.want {
				t.Fatalf("got %v want %v", r.Smoothed, tc.want)
			}
		})
	}
}

func TestDurationBounds(t *testing.T) {
	if DecodeACKDelay(100, 3) != 800*time.Microsecond || DecodeACKDelay(1<<62-1, 20) != maxDuration {
		t.Fatal("ACK delay decode")
	}
	r := NewRTT()
	r.Update(0, 0, 0, false)
	if r.LossDelay() != Granularity || r.PTO(false, false, 0, 0) != Granularity {
		t.Fatal("zero RTT must preserve timer granularity")
	}
	r.Update(maxDuration, 0, 0, true)
	if r.Smoothed < 0 || r.Variation < 0 || r.LossDelay() != maxDuration {
		t.Fatalf("overflow: %+v", r)
	}
	if r.PTO(true, true, maxDuration, 0) != maxDuration || r.PTO(false, false, 0, 63) != maxDuration {
		t.Fatal("PTO did not saturate")
	}
	if weighted(maxDuration, 7, maxDuration, 1) != maxDuration {
		t.Fatal("weighted overflow")
	}
}

func TestMinimumRTTAfterPersistentCongestion(t *testing.T) {
	r := NewRTT()
	r.Update(10*time.Millisecond, 0, 0, true)
	r.OnPersistentCongestion()
	before := r
	r.Update(-1, 0, 0, true)
	if r != before {
		t.Fatal("invalid sample consumed minimum refresh")
	}
	r.Update(100*time.Millisecond, 25*time.Millisecond, 25*time.Millisecond, true)
	if r.Minimum != 100*time.Millisecond || !r.Sampled || r.Smoothed != 21250*time.Microsecond {
		t.Fatalf("refresh: %+v", r)
	}
	r.Update(120*time.Millisecond, 0, 0, true)
	if r.Minimum != 100*time.Millisecond {
		t.Fatal("minimum refreshed twice")
	}
	r.Update(50*time.Millisecond, 0, 0, true)
	if r.Minimum != 50*time.Millisecond {
		t.Fatal("minimum did not decrease")
	}
}
