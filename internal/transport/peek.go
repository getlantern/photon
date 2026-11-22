package transport

import "github.com/getlantern/photon/internal/stream"

// PeekStream inspects contiguous receive data without consuming credit,
// releasing stream state, or marking EOF/reset as read.
func (c *Conn) PeekStream(id uint64, dst []byte) (int, error) {
	if s := c.completedReads[id]; s != nil {
		n, err := s.receive.Peek(dst)
		return n, receiveStreamError(id, err)
	}
	if err := c.check(); err != nil {
		return 0, err
	}
	s := c.streams.objects[id]
	if s == nil || s.receive == nil {
		return 0, stream.ErrStreamState
	}
	if s.readTerminal != nil {
		return 0, s.readTerminal
	}
	n, err := s.receive.Peek(dst)
	return n, receiveStreamError(id, err)
}
