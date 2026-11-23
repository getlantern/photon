package transport

import "github.com/getlantern/photon/internal/stream"

const partialResetControlCharge = 32

// Reservations count against admission without representing pending packets.
func (c *Conn) availableSendBytes() int {
	return maxSendBytes - c.queuedBytes - c.reservedResetBytes
}
func (c *Conn) releaseResetReservation(s *streamState) {
	if s.resetReserved {
		s.resetReserved = false
		c.reservedResetBytes -= partialResetControlCharge
	}
}

// SetReliableBoundary snapshots bytes committed so far. A boundary can increase
// until cancellation; cancellation freezes it. Unsupported peers ignore it.
func (c *Conn) SetReliableBoundary(id uint64) error {
	if err := c.check(); err != nil {
		return err
	}
	s := c.streams.objects[id]
	if s == nil || s.send == nil {
		return stream.ErrStreamState
	}
	if !c.peerResetCapability() || s.resetQueued || s.send.Complete() {
		return nil
	}
	s.reliableBoundary = s.send.Offset()
	return nil
}

// CancelWriteReliable implements application cancellation while STOP_SENDING
// continues to use ordinary cancellation and abandons the reliable prefix.
func (c *Conn) CancelWriteReliable(id, code uint64) error {
	if err := c.check(); err != nil {
		return err
	}
	s := c.streams.objects[id]
	if s == nil || s.send == nil {
		return stream.ErrStreamState
	}
	if s.resetQueued || s.send.Complete() {
		return nil
	}
	if s.reliableBoundary != 0 && c.peerAllowsPartialReset() {
		return c.CancelWriteAt(id, code, s.reliableBoundary)
	}
	return c.CancelWrite(id, code)
}

// PartialResetCapabilities reports independent local and remote receive support.
// It must be called on the connection owner, like other negotiated state accessors.
func (c *Conn) PartialResetCapabilities() (local, remote bool) {
	return c.config.Parameters.ResetStreamAt, c.peerResetCapability()
}
