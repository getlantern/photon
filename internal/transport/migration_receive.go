package transport

import (
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func hasNonProbing(frames []wire.Frame) bool {
	for _, frame := range frames {
		if !wire.ProbingFrame(frame) {
			return true
		}
	}
	return false
}

func (c *Conn) migrationPTO(app *pathApplication) time.Duration {
	delay := time.Duration(c.peerParams.MaxAckDelay) * time.Millisecond
	// The new tuple has no RTT estimate yet. Keep at least the initial one-second
	// PTO, or the established path's larger PTO, for candidate validation.
	return max(time.Second, app.sent.RTT.PTO(true, true, delay, 0))
}

// receiveMigrationCandidate supports client remote-address rebinding on servers.
// It authenticates before allocating candidate state or crediting a new tuple.
// Candidate packets are not recorded/ACKed; only validation and CID lifecycle
// frames are processed. Application data stays retransmittable until admission. A packet
// completing validation is admitted after a supported binding commit.
func (c *Conn) receiveMigrationCandidate(raw []byte, tuple pathTuple, now time.Time, datagramBytes int) error {
	return c.receiveMigrationCandidateECN(raw, tuple, now, datagramBytes, ECNUnknown)
}
func (c *Conn) receiveMigrationCandidateECN(raw []byte, tuple pathTuple, now time.Time, datagramBytes int, marking ECN) error {

	// Clients must discard packets from an unknown server address (RFC 9000 §9).
	if !c.config.Server {
		return nil
	}
	authenticated, err := c.authenticateMultipathPacketECN(raw, now, marking)
	if err != nil || authenticated == nil {
		return err
	}
	packet := &authenticated.packet
	p := c.multipath
	state := p.paths[packet.pathID]
	app := state.application
	if p.abandoned[packet.pathID] != nil || app.network == nil || *app.network == tuple || (packet.pathID != 0 && app.validation.state != PathValidated) {
		return nil
	}
	if app.migration != nil && app.migration.candidate != nil && app.migration.candidate.tuple != tuple {
		for _, frame := range packet.frames {
			if response, ok := frame.(wire.PathResponseFrame); ok {
				if err := c.receiveMigrationResponse(packet.pathID, response, now); err != nil {
					return err
				}
			}
		}
		// Proof can commit the probed tuple, but it cannot admit unrelated
		// application frames received on this other, unvalidated tuple.
		if app.migration.candidate == nil {
			return nil
		}
	}
	// Only the server's advertised preferred socket authorizes a local-address
	// change. The endpoint independently checks that this socket is registered.
	preferred := packet.pathID == 0 && app.network.local != tuple.local && matchesPreferredAddress(c.config.Parameters.PreferredAddress, tuple.local)
	if tuple.local.Addr().Is4() != tuple.remote.Addr().Is4() || (app.network.local != tuple.local && !preferred) || (app.network.remote.Addr() != tuple.remote.Addr() && !c.config.allowIPMigration && !preferred) {
		return nil
	}
	// Reusing our current sending CID at the new peer address relies on the
	// RFC 9000 section 9.5 exception: the peer reused an authenticated DCID from
	// the established tuple. Otherwise probes require an exclusive unused peer CID.
	freshCID := preferred || !state.local.receivedOn(packet.destinationCID, *app.network)
	if freshCID && !c.config.allowFreshCIDMigration {
		return nil
	}
	nonProbing := hasNonProbing(packet.frames)
	probingChallenge := false
	if !nonProbing {
		for _, frame := range packet.frames {
			if _, ok := frame.(wire.PathChallengeFrame); ok {
				probingChallenge = true
				break
			}
		}
	}

	if app.migration == nil {
		if !preferred && (!probingChallenge && !nonProbing || (nonProbing && app.space.haveNonProbing && packet.number <= app.space.highestNonProbing)) {
			return nil
		}
		app.migration, err = app.space.migrationSelection(*app.network)
		if err != nil {
			return err
		}
	}
	previous := app.migration.candidate
	var candidate *migrationCandidate
	if preferred {
		candidate, err = app.migration.observePreferredAuthenticated(tuple, packet.number, nonProbing, datagramBytes, now, c.migrationPTO(app))
	} else if probingChallenge && (app.migration.candidate == nil || app.migration.candidate.requiresNomination) {
		candidate, err = app.migration.observeProbingAuthenticated(tuple, packet.number, false, datagramBytes, now, c.migrationPTO(app), false)
	} else {
		candidate, err = app.migration.observeAuthenticated(tuple, packet.number, nonProbing, datagramBytes, now, c.migrationPTO(app))
	}
	if err != nil || candidate == nil {
		return err
	}
	if candidate != previous {
		candidate.freshCID = freshCID
	}
	if err := c.receiveMigrationCIDControl(packet, now); err != nil {
		return err
	}
	freshChallenges, err := app.recordCandidateChallenges(packet.number, packet.frames, now)
	if err != nil {
		return err
	}
	for _, frame := range packet.frames {
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
	if candidate.validation.state == PathValidated {
		committed, err := c.commitPeerRebinding(packet.pathID, candidate)
		if err != nil {
			return err
		}
		if committed {
			state.local.noteReceived(packet.destinationCID, tuple)
			decoded, err := c.recordMultipathPacket(authenticated, now)
			if err != nil || decoded == nil {
				return err
			}
			if err := c.dispatchMultipathPacketFrames(decoded, now, true); err != nil {
				return err
			}
			c.armPathRecovery(packet.pathID, now)
		}
	}
	return nil
}
