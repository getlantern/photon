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
