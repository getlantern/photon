package transport

import (
	"github.com/getlantern/photon/internal/wire"
	"time"
)

type pendingMultipathPacket struct {
	packet pathPacket
	at     time.Time
}

func (c *Conn) multipathNegotiated() bool {
	return c.config.multipathCapacity > 0 && c.config.Parameters.InitialMaxPathID != nil && c.peerParams != nil && c.peerParams.InitialMaxPathID != nil
}

// Retain complete authenticated packets so frame order and path-zero CID
// history survive the owner handoff. The count/byte caps include frame metadata.
func (c *Conn) retainMultipathBeforeActivation(cid []byte, pn uint64, frames []wire.Frame, eliciting bool, rawBytes int, now time.Time) error {
	return c.retainMultipathBeforeActivationECN(cid, pn, frames, eliciting, rawBytes, now, ECNUnknown)
}

func (c *Conn) retainMultipathBeforeActivationECN(cid []byte, pn uint64, frames []wire.Frame, eliciting bool, rawBytes int, now time.Time, marking ECN) error {

	for _, f := range frames {
		if close, ok := f.(wire.ConnectionCloseFrame); ok {
			return c.beginDraining(peerCloseError(close), now)
		}
		if _, ok := f.(wire.HandshakeDoneFrame); ok {
			if !c.complete {
				return c.fail(&TransportError{Code: 0xa, FrameType: f.Type(), Reason: "HANDSHAKE_DONE before TLS completion"})
			}
			c.confirmed = true
			c.discard(handshakeSpace)
			c.notifyHandshakeProgress()
		}
	}
	charge := rawBytes + len(frames)*64
	if len(c.pendingMultipath) >= 64 || charge > maxDeferredBytes-c.pendingMultipathBytes {
		return nil
	}
	s := &c.spaces[applicationSpace]
	previous, hadPrevious := s.received.Largest()
	receipt, err := s.received.RecordECN(pn, now, marking)
	if err != nil {
		return c.fail(err)
	}
	if receipt == ForgottenPacket {
		return nil
	}
	if eliciting {
		s.scheduleACK(applicationSpace, pn, previous, hadPrevious, marking, now, c.ackDelay())
	}
	if receipt != NewPacket {
		return nil
	}
	// Path-zero ACKs must keep handshake-completion output and its recovery
	// moving while the owner arranges activation. Other path ACKs need the
	// table's path/unsent validation and remain ordered with retained controls.
	remaining := frames[:0]
	for _, f := range frames {
		var ack *wire.AckFrame
		switch f := f.(type) {
		case wire.AckFrame:
			ack = &f
		case wire.PathAckFrame:
			if f.PathID == 0 {
				ack = &f.AckFrame
			}
		}
		if ack != nil {
			if err := c.handleAck(applicationSpace, *ack, now); err != nil {
				return err
			}
		} else {
			remaining = append(remaining, f)
		}
	}
	clear(frames[len(remaining):])
	frames = remaining
	if len(frames) == 0 {
		return nil
	}
	c.pendingMultipath = append(c.pendingMultipath, pendingMultipathPacket{packet: pathPacket{pathID: 0, destinationCID: clone(cid), number: pn, frames: frames, receipt: receipt}, at: now})
	c.pendingMultipathBytes += charge
	return nil
}
