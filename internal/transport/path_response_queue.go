package transport

import (
	"errors"
	"github.com/getlantern/photon/internal/wire"
)

// Reserve a small FIFO outside application memory charges for responses that
// otherwise cannot be admitted. Overflow retains the existing resource failure
// policy; no pending challenge token is overwritten or answered twice.
type pathResponseQueue struct {
	frames [8]wire.PathResponseFrame
	count  int
}

func (q *pathResponseQueue) push(f wire.PathResponseFrame) bool {
	if q.count == len(q.frames) {
		return false
	}
	q.frames[q.count] = f
	q.count++
	return true
}
func (q *pathResponseQueue) pop() {
	copy(q.frames[:], q.frames[1:q.count])
	q.count--
	q.frames[q.count] = wire.PathResponseFrame{}
}
func (c *Conn) queuePathResponse(id uint64, f wire.PathResponseFrame) error {
	// Responses queue at the head, so those waiting are a prefix: past
	// maxPathResponses a peer's further challenges go unanswered.
	if queue := c.multipath.paths[id].application.space.queue; len(queue) >= maxPathResponses {
		if _, ok := queue[maxPathResponses-1].frame.(wire.PathResponseFrame); ok {
			return nil
		}
	}
	if err := c.queueFrameOnPath(id, f); err != nil {
		if errors.Is(err, ErrSendBuffer) && c.multipath.paths[id].application.controlResponses.push(f) {
			return nil
		}
		return err
	}
	return nil
}

func (a *pathApplication) queuedPathResponse() bool {
	for _, item := range a.space.queue {
		if item.acked {
			continue
		}
		_, ok := item.frame.(wire.PathResponseFrame)
		return ok
	}
	return false
}
func (a *pathApplication) hasPathResponse() bool {
	return a.migrationResponse != nil || a.controlResponses.count > 0 || a.queuedPathResponse()
}
