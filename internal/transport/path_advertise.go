package transport

import (
	"maps"

	"github.com/getlantern/photon/internal/wire"
)

// advertisePathCIDs prepares local CIDs and queues their reliable advertisements
// on an existing sending path. Issuance and queue capacity are transactional;
// routing becomes active only when packet construction marks each CID sent.
func (c *Conn) advertisePathCIDs(id uint64) error {
	if err := c.check(); err != nil {
		return err
	}
	if c.multipath == nil || !c.confirmed {
		return ErrHandshakeUnconfirmed
	}
	p := c.multipath
	if id > min(p.localMax, p.peerMax) {
		return errPathInitiation
	}
	dest, ok := c.survivingPath(^uint64(0))
	if !ok {
		return errNoPathForControl
	}
	state, err := p.prepare(id)
	if err != nil {
		return err
	}
	staged := *state.local
	staged.active = maps.Clone(staged.active)
	staged.sent = maps.Clone(staged.sent)
	staged.used = maps.Clone(staged.used)
	frames, err := staged.issue(p.peerLimit)
	if err != nil {
		return err
	}
	items := make([]*sendItem, 0, len(frames))
	total := 0
	for _, base := range frames {
		f := wire.PathNewConnectionIDFrame{PathID: id, NewConnectionIDFrame: base}
		b, err := f.Append(nil)
		if err != nil {
			return err
		}
		size := max(len(b), 32)
		if size > c.availableSendBytes()-total {
			return ErrSendBuffer
		}
		total += size
		items = append(items, &sendItem{frame: f, bytes: size, queued: true})
	}
	if len(items) == 0 {
		return nil
	}
	// Preserve the shared lifetime registry object used by every path.
	maps.Copy(state.local.used, staged.used)
	staged.used = state.local.used
	*state.local = staged
	p.paths[id] = state
	s := p.paths[dest].application.space
	s.queue = append(s.queue, items...)
	c.queuedBytes += total
	return nil
}
