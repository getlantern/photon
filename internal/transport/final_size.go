package transport

import "github.com/getlantern/photon/internal/stream"

// WatchReceiveFinalSize installs a one-shot observer. A known final size is
// reported immediately, even if gaps remain before the final offset.
func (c *Conn) WatchReceiveFinalSize(id uint64, notify func(uint64)) error {
	if err := c.check(); err != nil {
		return err
	}
	s := c.streams.objects[id]
	if s == nil || s.receive == nil || !s.accepted || notify == nil || s.onReceiveFinalSize != nil {
		return stream.ErrStreamState
	}
	if final, known := s.receive.FinalSize(); known {
		notify(final)
		return nil
	}
	s.onReceiveFinalSize = notify
	return nil
}
