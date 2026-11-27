package transport

import "github.com/getlantern/photon/internal/stream"

const partialResetControlCharge = 32

// Reservations count against admission without representing pending packets.
func (c *Conn) availableSendBytes() int {
	return maxSendBytes - c.queuedBytes - c.reservedResetBytes
}

// availableStreamBytes is how much more stream data may be queued: up to
// streamSendLimit, and within availableSendBytes. Control frames use the
// latter alone, so a window that shrinks below what is already queued
// holds back only new stream data.
func (c *Conn) availableStreamBytes() int {
	return min(c.availableSendBytes(), c.streamSendLimit()-c.queuedBytes-c.reservedResetBytes)
}

// streamSendLimit follows the congestion window, summed over paths, so a
// connection buffers about what its paths can carry: a window grows only as
// the peer acknowledges data, so one that never does holds the connection to
// minStreamSendBytes.
func (c *Conn) streamSendLimit() int {
	var window uint64
	if c.multipath != nil {
		for _, state := range c.multipath.paths {
			if state.application != nil {
				window += state.application.congestion.Window()
			}
		}
	} else {
		window = c.congestion.Window()
	}
	return int(min(max(streamSendWindows*window, minStreamSendBytes), maxSendBytes-controlReserve))
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
