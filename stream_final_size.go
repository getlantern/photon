package quic

import "context"

// SetReceiveFinalSizeCallback reports the final offset learned from FIN or reset,
// even when earlier data is still missing. A known size is reported before this
// method returns. Otherwise the callback runs on the connection owner and must
// not block or call QUIC methods. Nil removes a pending callback. Connection
// closure before a final size is known does not invoke the callback.
func (s *ReceiveStream) SetReceiveFinalSizeCallback(callback func(int64)) {
	var known bool
	var size uint64
	_ = s.conn.waitReceive(context.Background(), func() (bool, error) {
		if s.rejected.Load() {
			return true, nil
		}
		if s.finalKnown {
			known, size = true, s.finalSize
		} else if s.conn.raw.Err() == nil {
			s.finalCallback = callback
		}
		return true, nil
	})
	// Already-known callbacks run outside the owner, allowing ordinary immediate
	// use without re-entering the owner's command queue.
	if known && callback != nil {
		callback(int64(size))
	}
}
