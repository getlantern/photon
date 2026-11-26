package recovery

import (
	"math"
	"time"
)

// Pacer implements a bounded token bucket at 1.25*cwnd/smoothed_rtt. Bursts
// are bounded to ten datagrams (and never more than cwnd), but always hold
// the largest packet asked about since a packet that large was last sent,
// so that packet can go. ACK-only packets bypass it; probes can bypass readiness but must
// still debit their bytes. Callers serialize access and commit Sent only
// after packet handoff.
type Pacer struct {
	last        time.Time
	tokens      float64
	initialized bool
	largest     float64 // the largest packet asked about
}

func (p *Pacer) Delay(now time.Time, window uint64, rtt time.Duration, size int) time.Duration {
	burst := float64(min(window, uint64(12000)))
	rtt = max(rtt, Granularity)
	rate := 1.25 * float64(window) / float64(rtt) // Bytes per nanosecond.
	return p.delay(now, rate, burst, size)
}

// DelayFor paces at the controller's own rate when it sets one, with bursts
// of its send quantum: 1 ms at that rate, between two minimum-size packets
// and 64 KB, which is never more than draft-ietf-ccwg-bbr's quantum, or the
// largest packet asked about if that is more. Otherwise it paces from the
// controller's window, as Delay does.
func (p *Pacer) DelayFor(now time.Time, c Controller, rtt time.Duration, size int) time.Duration {
	bps := c.PacingRate()
	if bps == 0 {
		return p.Delay(now, c.Window(), rtt, size)
	}
	rate := float64(bps) / float64(time.Second)
	burst := min(max(rate*float64(time.Millisecond), 2*1200), 64*1024)
	return p.delay(now, rate, burst, size)
}

func (p *Pacer) delay(now time.Time, rate, burst float64, size int) time.Duration {
	// The bucket must hold the largest packet, or it could never fill
	// enough to send it. The cap uses the largest packet asked about, not
	// this call's, so a check for a smaller packet between a large packet's
	// checks doesn't trim the tokens saved for it.
	p.largest = max(p.largest, float64(size))
	burst = max(burst, p.largest)
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
func (p *Pacer) Sent(size int) {
	p.tokens -= float64(size)
	// The largest packet has gone; later bursts follow the rate again.
	if float64(size) >= p.largest {
		p.largest = 0
	}
}
