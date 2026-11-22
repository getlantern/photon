package transport

import (
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// receiveLocalMigrationCandidate accepts validation and CID control on the
// nominated tuple, and response proof on other local tuples with the same peer.
// It does not record/ACK or dispatch application frames before
// the socket owner commits migration. A bounded response FIFO limits
// authenticated challenge floods; output uses candidate CID/credit/pacing gates.
func (c *Conn) receiveLocalMigrationCandidate(raw []byte, tuple pathTuple, now time.Time) error {
	if c.config.Server {
		return nil
	}
	authenticated, err := c.authenticateMultipathPacket(raw, now)
	if err != nil || authenticated == nil {
		return err
	}
	state := c.multipath.paths[authenticated.packet.pathID]
	app := state.application
	candidate := app.localMigration
	if c.multipath.abandoned[authenticated.packet.pathID] != nil || candidate == nil || candidate.tuple.remote != tuple.remote || candidate.expire(now) {
		return nil
	}
	if _, ok := state.peer.migrationID(candidate, candidate.peerSequence); !ok {
		return nil
	}
	if candidate.tuple != tuple {
		for _, frame := range authenticated.packet.frames {
			if response, ok := frame.(wire.PathResponseFrame); ok {
				candidate.responseProof(response, now)
			}
		}
		return nil
	}
	if err := c.receiveMigrationCIDControl(&authenticated.packet, now); err != nil {
		return err
	}
	if _, ok := state.peer.migrationID(candidate, candidate.peerSequence); !ok {
		return nil
	}
	freshChallenges, err := app.recordCandidateChallenges(authenticated.packet.number, authenticated.packet.frames, now)
	if err != nil {
		return err
	}
	for _, frame := range authenticated.packet.frames {
		switch f := frame.(type) {
		case wire.PathResponseFrame:
			candidate.response(tuple, f, now)
		case wire.PathChallengeFrame:
			if !freshChallenges {
				continue
			}
			if err := candidate.queueResponse(wire.PathResponseFrame{Data: f.Data}); err != nil {
				return err
			}
		}
	}
	return nil
}
