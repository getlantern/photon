package transport

import (
	"github.com/getlantern/photon/internal/stream"
)

// TryWriteStreamAll admits a complete slice without waiting for flow or queue
// credit. The owner serializes the preflight and commit. Portable chunks make
// the reservation independent of path selection or later MTU fallback.
func (c *Conn) TryWriteStreamAll(id uint64, data []byte) error {
	if err := c.SendError(id); err != nil {
		return err
	}
	s := c.streams.objects[id]
	if len(data) == 0 {
		return nil
	}
	if uint64(len(data)) > s.send.Limit()-s.send.Offset() || uint64(len(data)) > c.streams.send.Limit()-c.streams.send.Used() {
		return stream.ErrWouldBlock
	}
	available := c.availableStreamBytes()
	if len(data) > available {
		return stream.ErrWouldBlock
	}
	chunk := portableStreamPayload
	if c.sendingEarly() {
		chunk = portableEarlyStreamPayload
	}
	reserve := 0
	if !s.resetReserved && (c.peerResetCapability() || c.sendingEarly()) {
		reserve = partialResetControlCharge
	}
	frames := (len(data) + chunk - 1) / chunk
	if len(data)+frames*32+reserve > available {
		return stream.ErrWouldBlock
	}
	// All or nothing: the slice waits for room for all its frames, or, if
	// it alone needs more, for an empty queue.
	if queued := c.queuedFrames(); queued > 0 && queued+frames > maxQueuedFrames {
		return stream.ErrWouldBlock
	}
	if c.multipath != nil && c.confirmed {
		if _, ok := c.survivingPath(^uint64(0)); !ok {
			return c.fail(&TransportError{Code: 0x10, Reason: "no viable path for application output"})
		}
	}
	// Admitted whole: its chunks don't wait on the frame bound.
	c.admittedWhole = true
	defer func() { c.admittedWhole = false }()
	for len(data) > 0 {
		size := min(chunk, len(data))
		n, err := c.WriteStream(id, data[:size], false)
		if err != nil {
			return err
		}
		if n != size {
			return c.fail(ErrSendBuffer)
		}
		data = data[n:]
	}
	return nil
}
