package transport

import (
	"context"

	"github.com/getlantern/photon/internal/stream"
)

// StreamAcknowledged reports whether all currently committed STREAM frames have
// been acknowledged, including FIN if queued. It does not require a closed send
// side. The caller serializes further writes while using this as a delivery
// barrier. Resetting a stream is not successful delivery of its discarded bytes.
// Query before releasing engine storage; public handles retain terminal state.
func (c *Conn) StreamAcknowledged(id uint64) error {
	if err := c.check(); err != nil {
		return err
	}
	s := c.streams.objects[id]
	if s == nil || s.send == nil {
		return stream.ErrStreamState
	}
	if s.resetError != nil {
		return s.resetError
	}
	if s.sendTerminal != nil && s.sendTerminal != context.Canceled {
		return s.sendTerminal
	}
	if s.outstandingData != 0 {
		return stream.ErrWouldBlock
	}
	return nil
}
