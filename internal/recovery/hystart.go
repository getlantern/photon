package recovery

import "time"

// HyStart++ (RFC 9406) parameters. NewReno is paced, so slow start's growth
// per ACK has no limit (the RFC's L is infinite).
const (
	hystartMinRTTThresh  = 4 * time.Millisecond
	hystartMaxRTTThresh  = 16 * time.Millisecond
	hystartMinRTTDivisor = 8
	hystartRTTSamples    = 8
	cssGrowthDivisor     = 4
	cssRounds            = 5
)

// hystart ends NewReno's initial slow start when the RTT starts rising, a
// queue filling, rather than at the first loss: on a deep buffer that loss
// comes a queue's delay late, after slow start has doubled the window past
// what the path holds and lost a large part of it. A rise first enters
// conservative slow start (CSS), which grows a quarter as fast; if the RTT
// falls back below the rise, the rise was spurious and slow start resumes;
// after cssRounds rounds of CSS, slow start ends. quic-go's Reno sender
// runs the earlier HyStart, which ends slow start at the first rise.
//
// A round runs from an ACK until one acknowledges a packet sent after it,
// by send time. Packets sent in response to that ACK share its time, so
// their ACKs still fall in the round: a round may run slightly long, never
// short.
type hystart struct {
	active              bool
	roundStart          time.Time
	lastMin, currentMin time.Duration // zero until sampled
	samples             int
	css                 bool
	cssBaseline         time.Duration
	cssRoundsDone       int
	done                bool // slow start has ended; HyStart++ covers only the first
}

// onACK takes one ACK in slow start and reports whether slow start should
// end, after conservative slow start's rounds.
func (h *hystart) onACK(e ACKEvent) bool {
	if h.done {
		return false
	}
	if !h.active || h.roundEnded(e.Acked) {
		if h.active && h.css {
			h.cssRoundsDone++
			if h.cssRoundsDone >= cssRounds {
				h.done, h.css = true, false
				return true
			}
		}
		if h.active {
			h.lastMin = h.currentMin
		}
		h.active, h.roundStart = true, e.Now
		h.currentMin, h.samples = 0, 0
	}
	if rtt := e.Rate.RTT; rtt > 0 {
		if h.currentMin == 0 || rtt < h.currentMin {
			h.currentMin = rtt
		}
		h.samples++
	}
	if h.samples < hystartRTTSamples || h.currentMin == 0 || h.lastMin == 0 {
		return false
	}
	if !h.css {
		thresh := min(max(h.lastMin/hystartMinRTTDivisor, hystartMinRTTThresh), hystartMaxRTTThresh)
		if h.currentMin >= h.lastMin+thresh {
			h.css, h.cssBaseline, h.cssRoundsDone = true, h.currentMin, 0
		}
	} else if h.currentMin < h.cssBaseline {
		h.css, h.cssBaseline = false, 0
	}
	return false
}

// roundEnded reports whether acked holds an in-flight packet sent after the
// round began.
func (h *hystart) roundEnded(acked []Packet) bool {
	for _, p := range acked {
		if p.InFlight && p.SentAt.After(h.roundStart) {
			return true
		}
	}
	return false
}
