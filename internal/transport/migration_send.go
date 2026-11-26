package transport

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

// nextMigrationProbe shares the path's nonce/packet-number/key-usage domain,
// but sends to and charges the candidate tuple. Until migration commit has
// separate recovery treatment, probes conservatively use established congestion
// and pacing limits as well. They are tracked in flight, never as reliable data.
func (c *Conn) nextMigrationProbe(id uint64, now time.Time) ([]byte, pathTuple, error) {
	state := c.multipath.paths[id]
	app := state.application
	var candidate *migrationCandidate
	if app.migration != nil {
		app.migration.expire(now)
		candidate = app.migration.candidate
	}
	if err := c.retireMigrationReservations(id, candidate); err != nil && !errors.Is(err, ErrSendBuffer) {
		return nil, pathTuple{}, err
	}
	if candidate == nil {
		return nil, pathTuple{}, nil
	}
	return c.sealMigrationProbe(id, candidate, now, false)
}

// Immediate migration probes include PING to nominate the new source address;
// explicit-switch and preferred-address probes contain only probing frames.
// validation alone does not authorize ordinary traffic to switch bindings.
func (c *Conn) nextLocalMigrationProbe(id uint64, now time.Time) ([]byte, pathTuple, error) {
	app := c.multipath.paths[id].application
	candidate := app.localMigration
	if candidate == nil {
		return nil, pathTuple{}, nil
	}
	_, owned := c.multipath.paths[id].peer.migrationID(candidate, candidate.peerSequence)
	if candidate.expire(now) || !owned {
		if !owned {
			candidate.validation.state = PathValidationFailed
			candidate.failure = errMigrationState
			candidate.responsePending = nil
			candidate.responseTail = pathResponseQueue{}
		}
		err := c.cancelLocalMigration(id, candidate)
		if errors.Is(err, ErrSendBuffer) {
			err = nil
		}
		return nil, pathTuple{}, err
	}
	return c.sealMigrationProbe(id, candidate, now, true)
}

func (c *Conn) sealMigrationProbe(id uint64, candidate *migrationCandidate, now time.Time, local bool) ([]byte, pathTuple, error) {
	if c.pendingWrite != nil {
		return nil, pathTuple{}, nil
	}
	state := c.multipath.paths[id]
	app := state.application
	candidate.pacingAt = time.Time{}
	var frame wire.Frame
	challengePacket := candidate.responsePending == nil
	if challengePacket {
		challenge, ready, err := candidate.challenge(now)
		if err != nil || !ready {
			return nil, pathTuple{}, err
		}
		frame = challenge
	} else {
		frame = *candidate.responsePending
	}
	peerSequence := state.peer.current
	if candidate.freshCID {
		if !candidate.hasPeerSequence {
			var ok bool
			candidate.peerSequence, ok = state.peer.reserveMigrationID(candidate)
			if !ok {
				return nil, pathTuple{}, nil
			}
			candidate.hasPeerSequence = true
		}
		if _, ok := state.peer.migrationID(candidate, candidate.peerSequence); !ok {
			if app.migration != nil && app.migration.candidate == candidate {
				app.migration.candidate = nil
			}
			// Local ownership is checked by nextLocalMigrationProbe before this call.
			return nil, pathTuple{}, nil
		}
		peerSequence = candidate.peerSequence
	}
	peer := state.peer.active[peerSequence]
	if len(peer.cid) == 0 {
		return nil, pathTuple{}, nil
	}
	size := 1 + len(peer.cid) + 4 + 16 + 9
	if local && !candidate.preferred && !candidate.explicitSwitch && challengePacket {
		size++
	}
	if candidate.budget.CanSend(1200) {
		size = 1200
	}
	if !candidate.budget.CanSend(size) || !app.congestion.CanSend(app.sent.BytesInFlight, size, true, false) || !app.sent.CanRecord(recovery.Application, true) || app.sent.Outstanding(recovery.Application) >= maxSentPackets {
		return nil, pathTuple{}, nil
	}
	if delay := app.pacer.DelayFor(now, app.congestion, app.sent.RTT.Smoothed, size); delay > 0 {
		candidate.pacingAt = now.Add(delay)
		return nil, pathTuple{}, nil
	}
	if c.packetRoutes != nil && !c.packetRoutes.reserve(candidate.tuple) {
		return nil, pathTuple{}, nil
	}
	if err := c.prepareApplicationKeys(); err != nil {
		return nil, pathTuple{}, c.fail(err)
	}
	keys := c.writeKeys(applicationSpace)
	if keys == nil {
		return nil, pathTuple{}, nil
	}
	limit, _ := keys.UsageLimits()
	if c.sealed[applicationSpace] >= limit {
		if err := c.InitiateKeyUpdateAt(now); err != nil {
			return nil, pathTuple{}, c.fail(aeadLimitError())
		}
		keys = c.writeKeys(applicationSpace)
	}
	pn := app.space.nextPN
	if pn >= wire.MaxVarint {
		return nil, pathTuple{}, c.exhaustPacketNumbers()
	}
	first := c.greaseQUICBit(0x43)
	if c.applicationKeys.phase {
		first |= 4
	}
	header := append([]byte{first}, peer.cid...)
	header = binary.BigEndian.AppendUint32(header, uint32(pn))
	payload, err := frame.Append(c.payloadScratch[:0])
	if err != nil {
		return nil, pathTuple{}, c.fail(err)
	}
	if local && !candidate.preferred && !candidate.explicitSwitch && challengePacket {
		payload = append(payload, 1)
	} // PING
	payload = append(payload, make([]byte, size-len(header)-16-len(payload))...)
	out, err := keys.SealPathInto(c.sealStorage, header, payload, pn, uint32(id))
	if err != nil {
		return nil, pathTuple{}, c.fail(err)
	}
	c.sealed[applicationSpace]++
	if challenge, ok := frame.(wire.PathChallengeFrame); ok {
		if err := candidate.sent(challenge, len(out), now); err != nil {
			return nil, pathTuple{}, c.fail(err)
		}
	} else {
		if err := candidate.budget.Sent(len(out)); err != nil {
			return nil, pathTuple{}, c.fail(err)
		}
		candidate.popResponse()
	}
	if err := app.recordSent(recovery.Packet{Number: pn, SentAt: now, Bytes: len(out), AckEliciting: true, InFlight: true}); err != nil {
		return nil, pathTuple{}, c.fail(err)
	}
	app.pacer.Sent(len(out))
	peer.used = true
	state.peer.active[peerSequence] = peer
	c.sentIdleActivity(now, true)
	c.armPathRecovery(id, now)
	c.sentTotals.emitted(len(out))
	c.publishStatistics()
	return out, candidate.tuple, nil
}
