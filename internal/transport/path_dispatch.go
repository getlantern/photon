package transport

import (
	"errors"
	"fmt"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// handleMultipathPacket applies authenticated frames on an already established
// path. Network initiation/validation and path-control response scheduling are
// still separate; the live driver must not enable negotiation until those are
// wired in. Stream state and flow-control credit belong to the connection.
func (c *Conn) handleMultipathPacket(raw []byte, now time.Time) error {
	decoded, err := c.decodeMultipathPacket(raw, now)
	if err != nil || decoded == nil {
		return err
	}
	return c.dispatchMultipathPacket(decoded, now)
}

func (c *Conn) dispatchMultipathPacket(decoded *pathPacket, now time.Time) error {
	return c.dispatchMultipathPacketFrames(decoded, now, false)
}

// Candidate admission already queued challenges before committing its tuple.
// Other frames in the committing packet still require normal dispatch.
func (c *Conn) dispatchMultipathPacketFrames(decoded *pathPacket, now time.Time, challengesHandled bool) error {
	if decoded.receipt != NewPacket {
		return nil
	}
	for _, frame := range decoded.frames {
		switch f := frame.(type) {
		case wire.StopSendingFrame, wire.StreamFrame, wire.ResetStreamFrame, wire.ResetStreamAtFrame, wire.StreamDataBlockedFrame, wire.MaxStreamDataFrame, wire.MaxDataFrame, wire.MaxStreamsFrame, wire.DataBlockedFrame, wire.StreamsBlockedFrame:
			if err := c.handleStreamFrame(f); err != nil {
				return err
			}
		case wire.NewConnectionIDFrame, wire.PathNewConnectionIDFrame:
			if receivedPacketNumber(c.multipath.paths[decoded.pathID].application.candidateCIDControls, decoded.number) {
				continue
			}
			retire, err := c.multipath.receiveNew(f)
			if err != nil {
				return c.fail(err)
			}
			for _, response := range retire {
				if err := c.queuePathControl(decoded.pathID, response); err != nil {
					return c.fail(err)
				}
			}
			if zero := c.multipath.paths[0]; zero != nil && c.multipath.abandoned[0] == nil {
				c.peerCID = clone(zero.peer.active[zero.peer.current].cid)
			}
		case wire.RetireConnectionIDFrame, wire.PathRetireConnectionIDFrame:
			if receivedPacketNumber(c.multipath.paths[decoded.pathID].application.candidateCIDControls, decoded.number) {
				continue
			}
			changed, err := c.multipath.receiveRetire(f, decoded.destinationCID)
			if err != nil {
				return c.fail(err)
			}
			if changed {
				var id uint64
				if f, ok := f.(wire.PathRetireConnectionIDFrame); ok {
					id = f.PathID
				}
				replacements, err := c.multipath.issue(id)
				if err != nil {
					return c.fail(err)
				}
				for _, response := range replacements {
					if err := c.queuePathControl(decoded.pathID, response); err != nil {
						return c.fail(err)
					}
				}
			}
		case wire.PathChallengeFrame:
			if challengesHandled || receivedPacketNumber(c.multipath.paths[decoded.pathID].application.candidateChallenges, decoded.number) {
				continue
			}
			if c.multipath.abandoned[decoded.pathID] == nil {
				if err := c.queuePathResponse(decoded.pathID, wire.PathResponseFrame{Data: f.Data}); err != nil {
					return c.fail(err)
				}
			}
		case wire.PathResponseFrame:
			if err := c.receiveMigrationResponse(decoded.pathID, f, now); err != nil {
				return err
			}
			if err := c.handlePathResponseOnPath(decoded.pathID, f, now); err != nil {
				return c.fail(err)
			}
		case wire.PathAbandonFrame:
			maxDelay := time.Duration(c.peerParams.MaxAckDelay) * time.Millisecond
			pto := c.retentionPTO(true, maxDelay)
			if state := c.multipath.paths[f.PathID]; state != nil && state.application != nil {
				pto = state.application.sent.RTT.PTO(true, true, maxDelay, 0)
			}
			if err := c.multipath.validateAbandon(f, now, pto); err != nil {
				return c.fail(err)
			}
			if last, ok := c.survivingPath(^uint64(0)); ok && last == f.PathID {
				if _, other := c.survivingPath(last); !other {
					failure := &TransportError{Code: 0x10, FrameType: f.Type(), Reason: "no viable path after abandonment"}
					c.fail(failure)
					// Cache the final close before retiring the peer CID. No
					// subsequent packet construction may use this path's CIDs.
					if err := c.CloseTransportError(failure.Code, failure.FrameType, failure.Reason, now); err != nil {
						return err
					}
					if _, err := c.multipath.receiveAbandon(f, now, pto); err != nil {
						return err
					}
					return failure
				}
			}
			respond, err := c.multipath.receiveAbandon(f, now, pto)
			if err != nil {
				return c.fail(err)
			}
			if respond {
				err := c.queuePathControl(decoded.pathID, wire.PathAbandonFrame{PathID: f.PathID})
				if errors.Is(err, errNoPathForControl) {
					return c.fail(&TransportError{Code: 0x10, FrameType: f.Type(), Reason: "no viable path after abandonment"})
				}
				if err != nil {
					return c.fail(err)
				}
				if _, err := c.multipath.beginAbandon(f.PathID); err != nil {
					return c.fail(err)
				}
			}
			if err := c.evacuatePath(f.PathID); err != nil {
				return c.fail(err)
			}
		case wire.PathStatusFrame:
			if _, err := c.multipath.receiveStatus(f); err != nil {
				return c.fail(err)
			}
		case wire.MaxPathIDFrame:
			if err := c.multipath.updatePeerMaximum(f); err != nil {
				return c.fail(err)
			}
		case wire.PathsBlockedFrame:
			if err := c.multipath.receivePathsBlocked(f); err != nil {
				return c.fail(err)
			}
		case wire.PathCIDsBlockedFrame:
			if err := c.multipath.receivePathCIDsBlocked(f); err != nil {
				return c.fail(err)
			}
		case wire.DatagramFrame:
			c.receiveDatagram(f.Data)
		case wire.AckFrame, wire.PathAckFrame:
			var acknowledgedPath uint64
			if f, ok := f.(wire.PathAckFrame); ok {
				acknowledgedPath = f.PathID
			}
			maxDelay := time.Duration(c.peerParams.MaxAckDelay) * time.Millisecond
			exponent := uint8(c.peerParams.AckDelayExponent)
			state := c.multipath.paths[acknowledgedPath]
			limited := true
			if state != nil && state.application != nil {
				app := state.application
				limited = len(app.space.queue) == 0 && app.sent.BytesInFlight < app.congestion.Window()
			}
			events, err := c.multipath.receiveReliableACK(f, now, exponent, maxDelay, limited, func(item *sendItem) error {
				if err := c.ackStreamFrame(item.frame); err != nil {
					return err
				}
				c.queuedBytes -= item.bytes
				c.recycleStreamItem(item)
				return nil
			})
			c.loss.lost(events.Lost)
			c.loss.recovered(events)
			if err != nil {
				return c.fail(err)
			}
			c.noteKeyPhaseWriteACK(f, now)
			if state != nil && state.application != nil {
				app := state.application
				// Retain lost data in its path queue for the outgoing scheduler. Shared
				// items already delivered or queued elsewhere are not queued again.
				queueLostItems(app.space, app.space, events.Lost)
				c.armPathRecovery(acknowledgedPath, now)
			}
		case wire.NewTokenFrame:
			c.receiveAddressToken(f.Token, now)
		case wire.PingFrame, wire.HandshakeDoneFrame:
			// This entry point requires handshake confirmation.
		case wire.CryptoFrame:
			if err := c.tls.HandleCrypto(levels[applicationSpace], f.Offset, f.Data); err != nil {
				return c.fail(err)
			}
			if err := c.syncTLS(); err != nil {
				return err
			}
		case wire.ConnectionCloseFrame:
			return c.beginDraining(peerCloseError(f), now)
		default:
			return c.fail(fmt.Errorf("%w: multipath 0x%x", ErrUnsupportedFrame, f.Type()))
		}
	}
	return nil
}
