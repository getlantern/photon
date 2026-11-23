package transport

import (
	"fmt"
	"github.com/getlantern/photon/internal/stream"
)

type StreamID int64
type StreamErrorCode uint64

// StreamError identifies a stream cancellation and which endpoint requested it.
type StreamError struct {
	StreamID  StreamID
	ErrorCode StreamErrorCode
	Remote    bool
}

func (e *StreamError) Error() string {
	side := "local"
	if e.Remote {
		side = "remote"
	}
	return fmt.Sprintf("quic: stream %d canceled by %s endpoint with code 0x%x", e.StreamID, side, e.ErrorCode)
}

// Is matches the canceled stream, application code and initiating endpoint.
func (e *StreamError) Is(target error) bool {
	other, ok := target.(*StreamError)
	return ok && other != nil && e.StreamID == other.StreamID && e.ErrorCode == other.ErrorCode && e.Remote == other.Remote
}

// WatchSendClose registers a single bounded owner callback. Successful FIN
// queuing closes the application's write side before acknowledgement; buffer
// reclamation still waits for ACK and receive-side completion.
func (c *Conn) WatchSendClose(id uint64, notify func(error)) error {
	if err := c.check(); err != nil {
		return err
	}
	s := c.streams.objects[id]
	if s == nil || s.send == nil || !s.accepted || notify == nil || s.onSendClosed != nil {
		return stream.ErrStreamState
	}
	if s.sendTerminal != nil {
		notify(s.sendTerminal)
		return nil
	}
	s.onSendClosed = notify
	return nil
}
func (s *streamState) closeSend(err error) {
	if s.sendTerminal != nil {
		return
	}
	s.sendTerminal = err
	if s.onSendClosed != nil {
		notify := s.onSendClosed
		s.onSendClosed = nil
		notify(err)
	}
}

func receiveStreamError(id uint64, err error) error {
	if reset, ok := err.(*stream.ResetError); ok {
		return &StreamError{StreamID: StreamID(id), ErrorCode: StreamErrorCode(reset.Code), Remote: true}
	}
	return err
}

// WatchReceiveClose observes complete FIN data, peer reset, or local cancellation.
// Completion does not consume buffered bytes. The owner callback must not reenter
// the engine; connection shutdown is observed separately through Conn's context.
func (c *Conn) WatchReceiveClose(id uint64, notify func(error)) error {
	if err := c.check(); err != nil {
		return err
	}
	s := c.streams.objects[id]
	if s == nil || s.receive == nil || !s.accepted || notify == nil || s.onReceiveClosed != nil {
		return stream.ErrStreamState
	}
	if s.receiveTerminal != nil {
		notify(s.receiveTerminal)
		return nil
	}
	s.onReceiveClosed = notify
	return nil
}
func (s *streamState) closeReceive(err error) {
	if s.receiveTerminal != nil {
		return
	}
	s.receiveTerminal = err
	if s.onReceiveClosed != nil {
		notify := s.onReceiveClosed
		s.onReceiveClosed = nil
		notify(err)
	}
}
