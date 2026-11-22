package transport

import (
	"maps"
	"slices"

	"github.com/getlantern/photon/internal/wire"
)

// retireMigrationReservations releases canceled reservations and reliably retires
// IDs already used for probes. The current candidate is preserved. Queue pressure
// leaves all state unchanged so normal output can drain and retry this operation.
func (c *Conn) retireMigrationReservations(id uint64, keep *migrationCandidate) error {
	state := c.multipath.paths[id]
	if state == nil || c.multipath.abandoned[id] != nil {
		return nil
	}
	var sequences []uint64
	for seq, peer := range state.peer.active {
		if peer.reservation != nil && peer.reservation != keep && (state.application == nil || peer.reservation != state.application.localMigration) {
			sequences = append(sequences, seq)
		}
	}
	if len(sequences) == 0 {
		return nil
	}
	slices.Sort(sequences)
	staged := *state.peer
	staged.active = maps.Clone(staged.active)
	var items []*sendItem
	for _, seq := range sequences {
		candidate := staged.active[seq].reservation
		if !staged.cancelMigrationID(candidate, seq) {
			continue
		}
		if err := staged.retireReserved(candidate, seq); err != nil {
			return err
		}
		var frame wire.Frame = wire.PathRetireConnectionIDFrame{PathID: id, Sequence: seq}
		if id == 0 {
			frame = wire.RetireConnectionIDFrame{Sequence: seq}
		}
		items = append(items, &sendItem{frame: frame, queued: true, bytes: 32})
	}
	if len(items) == 0 {
		*state.peer = staged
		return nil
	}
	if len(items) > c.availableSendBytes()/32 {
		return ErrSendBuffer
	}
	dest, ok := c.survivingPath(^uint64(0))
	if !ok {
		return errNoPathForControl
	}
	space := c.multipath.paths[dest].application.space
	if space.discarded {
		return errNoPathForControl
	}
	*state.peer = staged
	space.queue = append(space.queue, items...)
	c.queuedBytes += 32 * len(items)
	return nil
}
