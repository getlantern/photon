package transport

import (
	"errors"
	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
	"io"
)

// ReleaseStream relinquishes the application's stream object after both sides
// terminate. It is explicit so unaccepted or unread data cannot disappear.
// Late peer frames for released IDs are ignored, without recreating buffers or
// charging flow credit twice. Partial-reset final/error validation is retained
// for a bounded set of recently released streams; other metadata ends at release.
func (c *Conn) ReleaseStream(id uint64) error {
	if err := c.check(); err != nil {
		return err
	}
	if c.streams.closed.Contains(id) {
		return nil
	}
	s := c.streams.objects[id]
	if s == nil {
		return stream.ErrStreamState
	}
	if !s.accepted || (s.send != nil && !s.send.Complete()) || (s.receive != nil && !s.receive.Drained()) {
		return stream.ErrWouldBlock
	}
	if err := c.streams.closed.Add(id); err != nil {
		return c.fail(err)
	}
	c.retireFrames(func(f wire.Frame) bool {
		switch v := f.(type) {
		case wire.StopSendingFrame:
			return v.StreamID == id
		case wire.MaxStreamDataFrame:
			return v.StreamID == id
		}
		return false
	})
	if s.onRelease != nil {
		var terminal error
		if s.receive != nil {
			terminal = io.EOF
			if s.readTerminal != nil {
				terminal = s.readTerminal
			} else if _, err := s.receive.Read(nil); err != nil {
				terminal = receiveStreamError(id, err)
			}
		}
		s.notifyRelease(terminal)
	}
	if s.receive != nil {
		if final, code, ok := s.receive.PartialResetInfo(); ok {
			if c.streams.partialClosed == nil {
				c.streams.partialClosed = new(partialResetHistory)
			}
			c.streams.partialClosed.add(id, final, code)
		}
	}
	c.releaseResetReservation(s)
	delete(c.streams.objects, id)
	local := (id&1 == 0) != c.config.Server
	if local {
		return nil
	}
	uni := id&2 != 0
	limit := c.streams.ids.ReceiveLimit(uni)
	if limit < stream.MaxStreams {
		limit++
		if err := c.queueCredit(wire.MaxStreamsFrame{Unidirectional: uni, Maximum: limit}); err != nil {
			return err
		}
		if err := c.streams.ids.UpdateReceiveLimit(uni, limit); err != nil {
			return c.fail(err)
		}
	}
	return nil
}

// AutoReleaseStream enables cleanup after acceptance, receive consumption (or
// cancellation/reset), and send acknowledgement. The callback must not reenter
// the engine. It receives the terminal read error before storage is removed.
func (c *Conn) AutoReleaseStream(id uint64, released func(error)) error {
	if released == nil {
		return c.AutoReleaseStreamWithSendError(id, nil)
	}
	return c.AutoReleaseStreamWithSendError(id, func(readErr, sendErr error) { released(readErr) })
}

// AutoReleaseStreamWithSendError also preserves a reset delivery failure after
// an earlier FIN closed the send context. Both errors are snapshots passed before
// object removal; the callback must not reenter the engine.
func (c *Conn) AutoReleaseStreamWithSendError(id uint64, released func(error, error)) error {
	if err := c.check(); err != nil {
		return err
	}
	s := c.streams.objects[id]
	if s == nil || !s.accepted || released == nil || s.onRelease != nil {
		return stream.ErrStreamState
	}
	s.onRelease = released
	return c.autoReleaseStream(id)
}
func (c *Conn) autoReleaseStream(id uint64) error {
	s := c.streams.objects[id]
	if s == nil || s.onRelease == nil {
		return nil
	}
	err := c.ReleaseStream(id)
	if errors.Is(err, stream.ErrWouldBlock) {
		return nil
	}
	return err
}

func (s *streamState) notifyRelease(readErr error) {
	var sendErr error
	if s.resetError != nil {
		sendErr = s.resetError
	}
	s.onRelease(readErr, sendErr)
}
