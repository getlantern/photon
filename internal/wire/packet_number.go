package wire

import "errors"

var ErrPacketNumber = errors.New("quic: invalid or exhausted packet number")

// DecodePacketNumber reconstructs a packet number using RFC 9000 Appendix A.3.
// expected is one greater than the largest authenticated packet number in this
// packet number space, or zero before any packet has been authenticated.
func DecodePacketNumber(truncated uint64, n int, expected uint64) (uint64, error) {
	if n < 1 || n > 4 || expected > MaxVarint+1 {
		return 0, ErrPacketNumber
	}
	window := uint64(1) << (8 * n)
	if truncated >= window {
		return 0, ErrPacketNumber
	}
	half := window / 2
	candidate := expected & ^(window-1) | truncated
	if candidate+half <= expected && candidate < (1<<62)-window {
		candidate += window
	} else if candidate > expected+half && candidate >= window {
		candidate -= window
	}
	if candidate > MaxVarint {
		return 0, ErrPacketNumber
	}
	return candidate, nil
}
