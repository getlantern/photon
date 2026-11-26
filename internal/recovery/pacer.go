package recovery

import (
	"math"
	"time"
)

// Pacer implements a bounded token bucket at 1.25*cwnd/smoothed_rtt. Bursts
// are bounded to ten datagrams (and never more than cwnd). ACK-only packets
// bypass it; probes can bypass readiness but must still debit their bytes.
// Callers serialize access and commit Sent only after packet handoff.
type Pacer struct {
	last        time.Time
	tokens      float64
	initialized bool
}

func (p *Pacer) Delay(now time.Time, window uint64, rtt time.Duration, size int) time.Duration {
	burst := float64(min(window, uint64(12000)))
	rtt = max(rtt, Granularity)
	rate := 1.25 * float64(window) / float64(rtt) // Bytes per nanosecond.
	return p.delay(now, rate, burst, size)
}

// DelayFor paces at the controller's own rate when it sets one, with bursts
// of its send quantum: 1 ms at that rate, between two maximum-size packets
// and 64 KB, as draft-ietf-ccwg-bbr does. Otherwise it paces from the
// controller's window, as Delay does.
func (p *Pacer) DelayFor(now time.Time, c Controller, rtt time.Duration, size int) time.Duration {
	bps := c.PacingRate()
	if bps == 0 {
		return p.Delay(now, c.Window(), rtt, size)
	}
	rate := float64(bps) / float64(time.Second)
	burst := min(max(rate*float64(time.Millisecond), 2*1452), 64*1024)
	return p.delay(now, rate, burst, size)
}

func (p *Pacer) delay(now time.Time, rate, burst float64, size int) time.Duration {
	if !p.initialized {
		p.tokens = burst
		p.last = now
		p.initialized = true
	}
	if now.After(p.last) {
		p.tokens = min(burst, p.tokens+float64(now.Sub(p.last))*rate)
		p.last = now
	} else {
		p.tokens = min(p.tokens, burst)
	}
	if p.tokens >= float64(size) {
		return 0
	}
	if rate <= 0 {
		return maxDuration
	}
	wait := math.Ceil((float64(size) - p.tokens) / rate)
	if wait >= float64(maxDuration) {
		return maxDuration
	}
	delay := max(time.Duration(wait), time.Nanosecond)
	if now.Before(p.last) {
		delay = add(delay, p.last.Sub(now))
	}
	return delay
}
func (p *Pacer) Sent(size int) { p.tokens -= float64(size) }
