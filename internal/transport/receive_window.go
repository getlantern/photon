package transport

import "time"

// receiveWindowSample tracks application consumption, never RESET_STREAM credit
// release. Its owner supplies validated current/maximum windows and a measured
// RTT. It proposes capacity only; buffer growth and advertised credit remain
// separate, fallible operations on the connection owner.
type receiveWindowSample struct {
	start    time.Time
	consumed uint64
}

func (s *receiveWindowSample) consume(n int, current, maximum uint64, rtt time.Duration, now time.Time) uint64 {
	if n <= 0 || current == 0 || maximum <= current {
		return current
	}
	if rtt <= 0 || now.IsZero() {
		*s = receiveWindowSample{}
		return current
	}
	if s.start.IsZero() || now.Before(s.start) {
		s.start, s.consumed = now, 0
		// The first read establishes the observation epoch, rather than claiming
		// that already-buffered data arrived during a zero-length sample.
		return current
	}
	// Clamp to the decision threshold so even a large application read cannot
	// overflow the sample or trigger multiple doublings in one observation.
	threshold := current/2 + current%2
	s.consumed += min(uint64(n), threshold-s.consumed)
	if s.consumed < threshold {
		return current
	}
	elapsed := now.Sub(s.start)
	s.start, s.consumed = now, 0
	// Avoid overflowing time.Duration when the caller supplies a large RTT.
	if elapsed > rtt && elapsed-rtt > rtt {
		return current
	}
	return current + min(current, maximum-current)
}

func (c *Conn) growReceiveWindows(s *streamState, n int, now time.Time) error {
	rtt := time.Duration(0)
	if c.recovery.RTT.Sampled {
		rtt = c.recovery.RTT.Smoothed
	}
	if c.multipath != nil && c.confirmed {
		rtt = 0
		for id, path := range c.multipath.paths {
			if c.multipath.abandoned[id] != nil || path.application == nil {
				continue
			}
			sample := path.application.sent.RTT
			if sample.Sampled && sample.Smoothed > 0 && (rtt == 0 || sample.Smoothed < rtt) {
				rtt = sample.Smoothed
			}
		}
	}
	if _, final := s.receive.FinalSize(); !final {
		window := s.windowSample.consume(n, s.window, c.config.MaxStreamReceiveWindow, rtt, now)
		if window > s.window {
			if err := s.receive.Grow(int(window)); err != nil {
				return err
			}
			s.window = window
		}
	}
	window := c.streams.windowSample.consume(n, c.streams.window, c.config.MaxConnectionReceiveWindow, rtt, now)
	if window > c.streams.window && c.allowConnectionWindowIncrease != nil && !c.allowConnectionWindowIncrease(window-c.streams.window) {
		return nil
	}
	c.streams.window = window
	return nil
}

// SetConnectionWindowPolicy must run on the serialized connection owner.
// The callback may veto growth but must not block or reenter the engine.
func (c *Conn) SetConnectionWindowPolicy(policy func(uint64) bool) {
	c.allowConnectionWindowIncrease = policy
}
