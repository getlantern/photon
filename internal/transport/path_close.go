package transport

import (
	"encoding/binary"
	"errors"
	"github.com/getlantern/photon/internal/packet"
	"net/netip"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// sealMultipathClose consumes one packet number on a surviving path. Closing
// retransmissions reuse the cached ciphertext, never repeat an AEAD operation.
func (c *Conn) sealMultipathClose(frame wire.ConnectionCloseFrame) (uint64, []byte, error) {
	id, ok := c.survivingPath(^uint64(0))
	if !ok {
		return 0, nil, &TransportError{Code: 0x10, Reason: "no viable path for connection close"}
	}
	app := c.multipath.paths[id].application
	if err := c.prepareApplicationKeys(); err != nil {
		return 0, nil, err
	}
	keys := c.writeKeys(applicationSpace)
	if keys == nil {
		return 0, nil, ErrHandshakeUnconfirmed
	}
	limit, _ := keys.UsageLimits()
	if c.sealed[applicationSpace] >= limit {
		return 0, nil, c.fail(aeadLimitError())
	}
	pn := app.space.nextPN
	if pn >= wire.MaxVarint {
		return 0, nil, c.exhaustPacketNumbers()
	}
	payload, err := frame.Append(nil)
	if err != nil {
		return 0, nil, err
	}
	state := c.multipath.paths[id]
	peer := state.peer.active[state.peer.current]
	first := c.greaseQUICBit(0x43)
	if c.applicationKeys.phase {
		first |= 4
	}
	header := append([]byte{first}, peer.cid...)
	header = binary.BigEndian.AppendUint32(header, uint32(pn))
	b, err := keys.SealPath(header, payload, pn, uint32(id))
	if err != nil {
		return 0, nil, c.fail(err)
	}
	c.sealed[applicationSpace]++
	app.space.nextPN++
	peer.used = true
	state.peer.active[state.peer.current] = peer
	return id, b, nil
}

// receiveClosingMultipath only authenticates bound paths and recognizes a
// peer close. It never dispatches stream/control work or accepts new paths.
func (c *Conn) receiveClosingMultipath(raw []byte, local, remote netip.AddrPort, now time.Time) error {
	if !now.Before(c.closing.until) {
		c.Close()
		return c.err
	}
	if c.multipath == nil || len(raw) == 0 || uint64(len(raw)) > c.config.Parameters.MaxUDPPayloadSize {
		return c.err
	}
	tuple, err := canonicalPathTuple(local, remote)
	if err != nil {
		return c.err
	}
	h, err := c.parseHeader(raw)
	if err != nil || h.Long || h.Type != wire.OneRTT {
		return c.err
	}
	id, ok := c.multipath.lookupLocal(h.DCID)
	if !ok {
		return c.err
	}
	app := c.multipath.paths[id].application
	if app == nil || app.network == nil || *app.network != tuple || app.space.received == nil {
		return c.err
	}
	keys := c.readKeys(applicationSpace)
	if keys == nil {
		return c.err
	}
	if err := c.checkIntegrityLimit(keys); err != nil {
		c.Close()
		return c.err
	}
	_, plain, pn, err := c.openApplicationPath(uint32(id), raw, h.PNOffset, app.space.received.Expected(), now)
	if err != nil {
		if authenticatedOpenError(err) {
			c.authenticatedPackets++
		}
		if errors.Is(err, packet.ErrAuthentication) {
			c.failedAuth++
			if c.failedAuth >= c.integrityLimit {
				c.Close()
				return c.err
			}
		}
		return c.err
	}
	c.authenticatedPackets++
	// Credit belongs to the arriving path, even if the cached close uses another.
	app.budget.Received(len(raw))
	c.closing.responseCredit.Received(len(raw))
	app.space.received.Record(pn, now)
	frames, _, err := parsePacketFrames(plain, wire.FrameContext{PacketType: wire.OneRTT, SenderIsServer: !c.config.Server, ResetStreamAtEnabled: c.config.Parameters.ResetStreamAt, MaxDatagramFrameSize: c.config.Parameters.MaxDatagramFrameSize, MultipathNegotiated: !c.multipath.baseOnly})
	if err == nil {
		for _, f := range frames {
			if _, ok := f.(wire.ConnectionCloseFrame); ok {
				return c.beginDraining(ErrPeerClosed, now)
			}
		}
	}
	if !now.Before(c.closing.nextResponse) {
		c.closing.pending = true
	}
	return c.err
}
