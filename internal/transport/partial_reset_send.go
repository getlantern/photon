package transport

import (
	"github.com/getlantern/photon/internal/stream"
)

// peerResetCapability uses remembered server support during early sending
// and current peer parameters once available.
func (c *Conn) peerResetCapability() bool {
	if c.sendingEarly() {
		remembered, ok := c.tls.ResumedTransport()
		return ok && remembered.ResetStreamAt
	}
	return c.peerParams != nil && c.peerParams.ResetStreamAt
}

func (c *Conn) peerAllowsPartialReset() bool {
	return (c.complete || c.sendingEarly() || c.serverApplicationReady()) && c.peerResetCapability()
}

// CancelWriteAt is the engine's partial-reset operation. Only a peer advertising
// reception, including remembered server support for 0-RTT, may be sent it.
// Rejected early stream generations are discarded without automatic replay.
func (c *Conn) CancelWriteAt(id, code, reliable uint64) error {
	if err := c.check(); err != nil {
		return err
	}
	if !c.peerAllowsPartialReset() {
		return ErrUnsupportedFrame
	}
	s := c.streams.objects[id]
	if s == nil || s.send == nil {
		return stream.ErrStreamState
	}
	frame, err := s.send.PrepareResetAt(code, reliable)
	if err != nil {
		return err
	}
	if s.partialReset != nil && *s.partialReset == frame {
		return nil
	}
	destination := &c.spaces[applicationSpace]
	if c.multipath != nil && c.confirmed {
		path, ok := c.survivingPath(^uint64(0))
		if !ok {
			return c.fail(&TransportError{Code: 0x10, Reason: "no viable path for partial reset"})
		}
		destination = c.multipath.paths[path].application.space
	}
	if destination.discarded {
		return stream.ErrSendClosed
	}
	charge := partialResetControlCharge
	if s.resetReserved {
		charge = 0 // The existing reservation funds the reset control.
	}
	plan, err := c.prepareStreamPrefix(id, reliable, charge)
	if err != nil {
		return err
	}
	retired := 0
	for _, item := range plan.items {
		if item.done {
			retired++
		}
	}
	if retired > s.outstandingData {
		return errStreamDelivery
	}
	// All recoverable validation/admission checks precede the first mutation.
	// The serialized owner cannot change paths or process ACKs during commit.
	if _, err := s.send.ResetAt(code, reliable); err != nil {
		return err
	}
	plan.commit(c)
	c.retireStreamDataBlocked(id)
	c.releaseConnectionBlocked(s)
	s.outstandingData -= retired
	s.partialReset = &frame
	s.resetQueued = true
	c.releaseResetReservation(s)
	c.queuedBytes += partialResetControlCharge
	destination.queue = append(destination.queue, &sendItem{frame: frame, queued: true, bytes: partialResetControlCharge})
	if s.outstandingData == 0 {
		// Every retained logical item is covered: unlike a highest ACKed offset,
		// this cannot skip holes between earlier writes or MTU-split fragments.
		if err := s.send.AcknowledgePrefix(frame.ReliableSize); err != nil {
			return c.fail(err)
		}
	}
	if s.resetError == nil {
		s.resetError = &StreamError{StreamID: StreamID(id), ErrorCode: StreamErrorCode(frame.ErrorCode)}
	}
	s.closeSend(s.resetError)
	return c.autoReleaseStream(id)
}
