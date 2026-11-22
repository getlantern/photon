package transport

import (
	"errors"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

type pathPacket struct {
	pathID         uint64
	destinationCID []byte
	number         uint64
	frames         []wire.Frame
	receipt        Receipt
}

// authenticatedPathPacket retains frame evidence before address admission.
// Authentication may update key-phase, integrity and idle state, but this stage
// does not record receipt, queue ACKs, dispatch frames or grant address credit.
type authenticatedPathPacket struct {
	marking      ECN
	packet       pathPacket
	ackEliciting bool
}

// decodeMultipathPacket is the ordinary admitted-path receive operation.
func (c *Conn) decodeMultipathPacket(raw []byte, now time.Time) (*pathPacket, error) {
	return c.decodeMultipathPacketECN(raw, now, ECNUnknown)
}
func (c *Conn) decodeMultipathPacketECN(raw []byte, now time.Time, marking ECN) (*pathPacket, error) {

	authenticated, err := c.authenticateMultipathPacketECN(raw, now, marking)
	if err != nil || authenticated == nil {
		return nil, err
	}
	return c.recordMultipathPacket(authenticated, now)
}

// authenticateMultipathPacket permits address selection to inspect authenticated
// frame evidence before committing receive history. A nil result means discard.
func (c *Conn) authenticateMultipathPacket(raw []byte, now time.Time) (*authenticatedPathPacket, error) {
	return c.authenticateMultipathPacketECN(raw, now, ECNUnknown)
}
func (c *Conn) authenticateMultipathPacketECN(raw []byte, now time.Time, marking ECN) (*authenticatedPathPacket, error) {
	if marking > ECNCE && marking != ECNUnknown {
		return nil, errors.New("quic: invalid ECN codepoint")
	}

	if err := c.check(); err != nil {
		return nil, err
	}
	if c.multipath == nil || !c.confirmed || len(raw) == 0 || uint64(len(raw)) > c.config.Parameters.MaxUDPPayloadSize {
		return nil, nil
	}
	h, err := c.parseHeader(raw)
	if err != nil || h.Type != wire.OneRTT || h.Long {
		return nil, nil
	}
	id, ok := c.multipath.lookupLocal(h.DCID)
	if !ok {
		return nil, nil
	}
	state := c.multipath.paths[id]
	if state.application == nil {
		return nil, nil
	}
	app := state.application
	keys := c.readKeys(applicationSpace)
	if keys == nil {
		return nil, nil
	}
	if err := c.checkIntegrityLimit(keys); err != nil {
		return nil, err
	}
	_, plain, pn, err := c.openApplicationPath(uint32(id), raw, h.PNOffset, max(app.space.authenticatedExpected, app.space.received.Expected()), now)
	if err != nil {
		return nil, c.packetOpenFailure(err)
	}
	app.space.authenticatedExpected = max(app.space.authenticatedExpected, pn+1)
	c.authenticatedPackets++
	c.receivedIdleActivity(now)
	frames, eliciting, err := parsePacketFrames(plain, wire.FrameContext{PacketType: wire.OneRTT, SenderIsServer: !c.config.Server, ResetStreamAtEnabled: c.config.Parameters.ResetStreamAt, MaxDatagramFrameSize: c.config.Parameters.MaxDatagramFrameSize, MultipathNegotiated: !c.multipath.baseOnly})
	if err != nil {
		return nil, c.fail(err)
	}
	return &authenticatedPathPacket{marking: marking,
		packet:       pathPacket{destinationCID: clone(h.DCID), pathID: id, number: pn, frames: frames},
		ackEliciting: eliciting,
	}, nil
}

// recordMultipathPacket commits only after the owner admits the packet's tuple.
// Copy metadata so admission does not erase evidence in the authentication result.
func (c *Conn) recordMultipathPacket(authenticated *authenticatedPathPacket, now time.Time) (*pathPacket, error) {
	if err := c.check(); err != nil {
		return nil, err
	}
	if authenticated == nil || c.multipath == nil {
		return nil, errPathBinding
	}
	decoded := authenticated.packet
	state := c.multipath.paths[decoded.pathID]
	if state == nil || state.application == nil {
		return nil, errPathBinding
	}
	app := state.application
	receipt, err := app.space.received.RecordECN(decoded.number, now, authenticated.marking)
	if err != nil {
		return nil, c.fail(err)
	}
	if receipt == ForgottenPacket {
		return nil, nil
	}
	if authenticated.ackEliciting {
		app.space.ackPending = true
	}
	decoded.receipt = receipt
	if receipt != NewPacket {
		decoded.frames = nil
	}
	return &decoded, nil
}
