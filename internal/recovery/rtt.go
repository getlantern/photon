// Package recovery implements QUIC loss recovery independently of packet I/O.
package recovery

import (
	"math"
	"time"
)

const InitialRTT = 333 * time.Millisecond
const Granularity = time.Millisecond
const maxDuration = time.Duration(math.MaxInt64)

// RTT holds path-level estimates shared by all packet number spaces.
type RTT struct {
	refreshMinimum                       bool
	Latest, Minimum, Smoothed, Variation time.Duration
	Sampled                              bool
}

func NewRTT() RTT { return RTT{Smoothed: InitialRTT, Variation: InitialRTT / 2} }

// weighted computes a nonnegative weighted average without overflowing duration.
func weighted(a time.Duration, aw int64, b time.Duration, bw int64) time.Duration {
	d := time.Duration(aw + bw)
	return (a/d)*time.Duration(aw) + (b/d)*time.Duration(bw) + ((a%d)*time.Duration(aw)+(b%d)*time.Duration(bw))/d
}
func add(a, b time.Duration) time.Duration {
	if b > maxDuration-a {
		return maxDuration
	}
	return a + b
}
func multiply(a time.Duration, n uint64) time.Duration {
	if n == 0 || a == 0 {
		return 0
	}
	if n > uint64(maxDuration/a) {
		return maxDuration
	}
	return a * time.Duration(n)
}

// Update applies RFC 9002 section 5.3. Initial/Handshake callers supply zero
// delay; application callers supply the decoded peer delay. MaxAckDelay only
// constrains the adjustment after handshake confirmation. Negative local clock
// samples are ignored; zero is a valid sample when timer resolution is coarse.
func (r *RTT) Update(sample, ackDelay, maxAckDelay time.Duration, confirmed bool) {
	if sample < 0 {
		return
	}
	r.Latest = sample
	if !r.Sampled {
		r.refreshMinimum = false
		r.Minimum = sample
		r.Smoothed = sample
		r.Variation = sample / 2
		r.Sampled = true
		return
	}
	if r.refreshMinimum {
		r.Minimum = sample
		r.refreshMinimum = false
	} else {
		r.Minimum = min(r.Minimum, sample)
	}
	ackDelay = max(ackDelay, 0)
	if confirmed {
		ackDelay = min(ackDelay, max(maxAckDelay, 0))
	}
	adjusted := sample
	if sample-r.Minimum >= ackDelay {
		adjusted -= ackDelay
	}
	difference := r.Smoothed - adjusted
	if difference < 0 {
		difference = -difference
	}
	r.Variation = weighted(r.Variation, 3, difference, 1)
	r.Smoothed = weighted(r.Smoothed, 7, adjusted, 1)
}
func (r *RTT) LossDelay() time.Duration {
	base := max(r.Latest, r.Smoothed)
	return max(add(base, base/8), Granularity)
}

// PTO returns a duration only; packet-space selection and arming belong to the
// recovery timer. Application max_ack_delay is included only after confirmation.
func (r *RTT) PTO(application, confirmed bool, maxAckDelay time.Duration, backoff uint32) time.Duration {
	duration := add(r.Smoothed, max(multiply(r.Variation, 4), Granularity))
	if application && confirmed {
		duration = add(duration, max(maxAckDelay, 0))
	}
	if backoff >= 63 {
		return maxDuration
	}
	return multiply(duration, uint64(1)<<backoff)
}

// DecodeACKDelay saturates instead of allowing peer input to overflow duration.
func DecodeACKDelay(encoded uint64, exponent uint8) time.Duration {
	if exponent > 20 {
		return maxDuration
	}
	maxUnits := uint64(maxDuration / time.Microsecond)
	if encoded > maxUnits>>exponent {
		return maxDuration
	}
	return time.Duration(encoded<<exponent) * time.Microsecond
}

// OnPersistentCongestion refreshes the path minimum on the next valid RTT
// sample (RFC 9002 section 5.2). Keep Sampled and the smoothed estimates: this
// is not a new-path initialization and must not erase loss-history eligibility.
func (r *RTT) OnPersistentCongestion() { r.refreshMinimum = true }
