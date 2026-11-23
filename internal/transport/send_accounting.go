package transport

// sendAccounting records committed packet emissions, including repeated close
// packets and retransmissions. It excludes UDP framing. Commitment precedes the
// socket write: subsequent local rejection does not erase the attempted send.
// The connection owner serializes all updates.
type sendAccounting struct {
	packets, bytes uint64
}

func (s *sendAccounting) emitted(size int) {
	s.packets++
	s.bytes += uint64(size)
}
