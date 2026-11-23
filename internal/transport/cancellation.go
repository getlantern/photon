package transport

import (
	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

func (c *Conn) CancelWrite(id, code uint64) error { return c.cancelWrite(id, code, false) }
func (c *Conn) cancelWrite(id, code uint64, remote bool) error {
	if err := c.check(); err != nil {
		return err
	}
	if code > wire.MaxVarint {
		return wire.ErrVarintRange
	}
	s := c.streams.objects[id]
	if s == nil || s.send == nil {
		return stream.ErrStreamState
	}
	if (s.resetQueued && s.partialReset == nil) || s.send.Complete() {
		return nil
	}
	frame, err := s.send.Reset(code)
	if err != nil {
		return err
	}
	c.retireFrames(func(f wire.Frame) bool {
		switch v := f.(type) {
		case wire.StreamFrame:
			return v.StreamID == id
		case wire.ResetStreamAtFrame:
			return v.StreamID == id
		case wire.StreamDataBlockedFrame:
			return v.StreamID == id
		}
		return false
	})
	c.releaseConnectionBlocked(s)
	c.releaseResetReservation(s)
	s.partialReset = nil
	s.outstandingData = 0
	s.resetQueued = true
	if err := c.enqueue(applicationSpace, frame, 32); err != nil {
		return err
	}
	if s.resetError == nil {
		s.resetError = &StreamError{StreamID: StreamID(id), ErrorCode: StreamErrorCode(frame.ErrorCode), Remote: remote}
	}
	s.closeSend(s.resetError)
	return nil
}
func (c *Conn) CancelRead(id, code uint64) error {
	if s := c.completedReads[id]; s != nil {
		if code > wire.MaxVarint {
			return wire.ErrVarintRange
		}
		delete(c.completedReads, id)
		if s.onRelease != nil {
			s.notifyRelease(&StreamError{StreamID: StreamID(id), ErrorCode: StreamErrorCode(code)})
		}
		return nil
	}
	if err := c.check(); err != nil {
		return err
	}
	if code > wire.MaxVarint {
		return wire.ErrVarintRange
	}
	s := c.streams.objects[id]
	if s == nil || s.receive == nil {
		return stream.ErrStreamState
	}
	if s.readTerminal != nil || s.receive.Drained() {
		return nil
	}
	s.readCanceled = &StreamError{StreamID: StreamID(id), ErrorCode: StreamErrorCode(code)}
	s.readTerminal = s.readCanceled
	s.closeReceive(s.readCanceled)
	if s.receive.Complete() {
		s.receive.DiscardComplete()
		if err := c.updateReceiveCredit(id, s); err != nil {
			return err
		}
		return c.autoReleaseStream(id)
	}
	return c.enqueue(applicationSpace, wire.StopSendingFrame{StreamID: id, ErrorCode: code}, 32)
}
func (c *Conn) retireStreamStop(id uint64) {
	c.retireFrames(func(f wire.Frame) bool { v, ok := f.(wire.StopSendingFrame); return ok && v.StreamID == id })
}
func (c *Conn) ackStreamFrame(f wire.Frame) error {
	switch v := f.(type) {
	case wire.StreamFrame:
		s := c.streams.objects[v.StreamID]
		if s == nil || s.send == nil {
			return nil
		}
		s.outstandingData--
		if s.outstandingData == 0 {
			if s.partialReset != nil {
				if err := s.send.AcknowledgePrefix(s.partialReset.ReliableSize); err != nil {
					return err
				}
			} else if s.finQueued {
				s.send.AcknowledgeData()
			}
		}
		if s.send.Complete() {
			c.releaseResetReservation(s)
		}
		return c.autoReleaseStream(v.StreamID)
	case wire.ResetStreamAtFrame:
		if s := c.streams.objects[v.StreamID]; s != nil && s.send != nil {
			if err := s.send.AcknowledgeResetAt(v); err != nil {
				return err
			}
		}
		return c.autoReleaseStream(v.StreamID)
	case wire.ResetStreamFrame:
		if s := c.streams.objects[v.StreamID]; s != nil && s.send != nil {
			s.send.AcknowledgeReset()
		}
		return c.autoReleaseStream(v.StreamID)
	}
	return nil
}
