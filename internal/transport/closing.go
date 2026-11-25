package transport

import (
	"bytes"
	"errors"
	"time"

	"github.com/getlantern/photon/internal/packet"
	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

var ErrLocalClosed = errors.New("quic: connection closed locally")
var ErrHandshakeUnconfirmed = errors.New("quic: handshake not confirmed")

type closingState struct {
	pathID              uint64
	network             *pathTuple
	budget              *Amplification
	responseCredit      Amplification
	sent                bool
	packets             [][]byte
	nextPacket          int
	until, nextResponse time.Time
	interval            time.Duration
	pending             bool
}

func (c *Conn) Closing() bool { return c.closing != nil && !c.closed }

// CloseWithError initiates an application close. Before handshake confirmation,
// it sends at available protection levels, converting handshake-level copies
// to APPLICATION_ERROR without a reason.
// NextDatagram must still be serviced to send close responses until the deadline.
// Close remains immediate local disposal.
func (c *Conn) CloseWithError(code uint64, reason string, now time.Time) error {
	if c.Closing() {
		return nil
	}
	if err := c.check(); err != nil {
		return err
	}
	if code > wire.MaxVarint {
		return wire.ErrVarintRange
	}
	frame := wire.ConnectionCloseFrame{Application: true, ErrorCode: code, Reason: []byte(reason[:min(len(reason), 900)])}
	return c.startClosing(frame, now, &ApplicationError{ErrorCode: ApplicationErrorCode(code), ErrorMessage: reason, cause: ErrLocalClosed})
}

// CloseTransportError can encode a previously reported protocol failure while
// its packet keys remain available. The caller supplies the transport code and
// triggering frame. Before confirmation, send at every available protection
// level. Confirmed connections use only 1-RTT.
func (c *Conn) CloseTransportError(code, trigger uint64, reason string, now time.Time) error {
	if c.Closing() {
		return nil
	}
	if c.closed || c.Draining() || !c.started {
		return c.check()
	}
	if code > wire.MaxVarint || trigger > wire.MaxVarint {
		return wire.ErrVarintRange
	}
	return c.startClosing(wire.ConnectionCloseFrame{ErrorCode: code, TriggerFrame: trigger, Reason: []byte(reason[:min(len(reason), 900)])}, now, &ConnectionTransportError{ErrorCode: TransportErrorCode(code), FrameType: trigger, ErrorMessage: reason, cause: ErrLocalClosed})
}

// sealClosePacket consumes a fresh packet number exactly once. Retransmissions
// reuse the protected result rather than invoking AEAD with that nonce again.
func (c *Conn) sealClosePacket(frame wire.ConnectionCloseFrame, space int) ([]byte, error) {
	if frame.Application && space != applicationSpace {
		frame = wire.ConnectionCloseFrame{ErrorCode: 0xc}
	}
	payload, err := frame.Append(nil)
	if err != nil {
		return nil, err
	}
	keys := c.writeKeys(space)
	limit, _ := keys.UsageLimits()
	counter := &c.sealed[space]
	if space == applicationSpace && c.sendingEarly() {
		counter = &c.earlySealed
	}
	if *counter >= limit {
		return nil, c.fail(aeadLimitError())
	}
	pn := c.spaces[space].nextPN
	if pn >= wire.MaxVarint {
		return nil, c.exhaustPacketNumbers()
	}
	header, err := c.makeHeader(space, pn, len(payload)+16)
	if err != nil {
		return nil, err
	}
	if space == initialSpace {
		for len(header)+len(payload)+16 < 1200 {
			payload = append(payload, make([]byte, 1200-len(header)-len(payload)-16)...)
			header, err = c.makeHeader(space, pn, len(payload)+16)
			if err != nil {
				return nil, err
			}
		}
		if excess := len(header) + len(payload) + 16 - 1200; excess > 0 {
			payload = payload[:len(payload)-excess]
			header, err = c.makeHeader(space, pn, len(payload)+16)
			if err != nil {
				return nil, err
			}
		}
	}
	packet, err := keys.Seal(header, payload, pn)
	if err != nil {
		return nil, c.fail(err)
	}
	*counter++
	c.spaces[space].nextPN++
	return packet, nil
}

func (c *Conn) startClosing(frame wire.ConnectionCloseFrame, now time.Time, cause error) error {
	if c.integrityLimit != 0 && c.failedAuth >= c.integrityLimit {
		return c.fail(aeadLimitError())
	}
	var packets [][]byte
	highest := initialSpace
	var closePath uint64
	if c.multipath != nil && c.confirmed {
		id, packet, err := c.sealMultipathClose(frame)
		if err != nil {
			return err
		}
		closePath = id
		packets = append(packets, packet)
		highest = applicationSpace
	}
	for space := initialSpace; c.multipath == nil && space <= applicationSpace; space++ {
		if (c.confirmed && space != applicationSpace) || c.writeKeys(space) == nil {
			continue
		}
		packet, err := c.sealClosePacket(frame, space)
		if err != nil {
			return err
		}
		packets = append(packets, packet)
		highest = space
	}
	if len(packets) == 0 {
		return ErrHandshakeUnconfirmed
	}
	maxDelay := 25 * time.Millisecond
	if c.peerParams != nil {
		maxDelay = time.Duration(c.peerParams.MaxAckDelay) * time.Millisecond
	}
	pto := c.retentionPTO(highest == applicationSpace, maxDelay)
	c.closing = &closingState{packets: packets, until: now.Add(triple(pto)), interval: max(pto, time.Millisecond), pending: true}
	if c.pendingWrite != nil {
		c.pendingWrite.release()
	}
	c.closing.budget = &c.budget
	if c.multipath != nil {
		app := c.multipath.paths[closePath].application
		c.closing.pathID, c.closing.network, c.closing.budget = closePath, app.network, app.budget
		for _, state := range c.multipath.paths {
			if app := state.application; app != nil {
				app.space.queue, app.space.sent = nil, nil
				app.space.clearACK()
				app.sent.Discard(recovery.Application)
				app.timer.DiscardKeys()
				*app.validation = pathValidation{}
				app.probePending = false
				app.migration = nil
				app.localMigration = nil
				app.migrationResponse = nil
				app.pacingAt = time.Time{}
			}
		}
	}
	c.fail(cause)
	// No more stream or recovery work is useful. Retain keys only to recognize a
	// peer close and transition to draining, plus routing IDs until retirement.
	for i := range c.spaces {
		c.spaces[i].queue = nil
		c.spaces[i].sent = nil
		c.spaces[i].clearACK()
		c.recovery.Discard(recovery.Space(i))
	}
	c.streams = streamSet{}
	c.validation = pathValidation{}
	c.datagrams = datagramBuffer{}
	c.pendingDatagrams = 0
	c.queuedBytes = 0
	c.reservedResetBytes = 0
	c.deferred = nil
	c.deferredBytes = 0
	c.pendingMultipath = nil
	c.pendingMultipathBytes = 0
	c.timer.DiscardKeys()
	return nil
}
func (c *Conn) nextClosingDatagram(now time.Time) ([]byte, error) {
	s := c.closing
	if !now.Before(s.until) {
		c.Close()
		return nil, c.err
	}
	if !s.pending || now.Before(s.nextResponse) {
		return nil, nil
	}
	packet := s.packets[s.nextPacket]
	if !s.budget.CanSend(len(packet)) {
		return nil, nil
	}
	if s.sent && !s.responseCredit.CanSend(len(packet)) {
		return nil, nil
	}
	if err := s.budget.Sent(len(packet)); err != nil {
		return nil, err
	}
	if s.sent {
		s.responseCredit.Sent(len(packet))
	}
	s.nextPacket++
	if s.nextPacket == len(s.packets) {
		s.nextPacket = 0
		s.sent = true
		s.pending = false
		s.nextResponse = now.Add(s.interval)
	}
	// RFC 9000 permits identical protected close retransmissions. Copy so the
	// adapter cannot mutate the retained packet, and never reseal its nonce.
	c.sentTotals.emitted(len(packet))
	c.publishStatistics()
	return clone(packet), nil
}
func (c *Conn) receiveClosing(data []byte, now time.Time) (result error) {
	if !now.Before(c.closing.until) {
		c.Close()
		return c.err
	}
	if len(data) == 0 || uint64(len(data)) > c.config.Parameters.MaxUDPPayloadSize {
		return c.err
	}
	authenticated := false
	defer func() {
		if !authenticated && !c.closed && !c.Draining() && c.isStatelessReset(data) {
			result = c.beginDraining(ErrStatelessReset, now)
		}
	}()
	// This single-path engine relies on the adapter's peer-address association.
	first, err := c.parseHeader(data)
	if err != nil || !c.acceptCID(first) {
		return c.err
	}
	if c.config.Server && first.Type == wire.Initial && len(data) < 1200 {
		return c.err
	}
	c.budget.Received(len(data))
	c.closing.responseCredit.Received(len(data))
	for rest := data; len(rest) > 0; {
		h, err := c.parseHeader(rest)
		if err != nil {
			break
		}
		raw := rest[:h.PacketLen]
		rest = rest[h.PacketLen:]
		if !bytes.Equal(h.DCID, first.DCID) || !c.acceptCID(h) || (h.Long && h.Version != c.config.Version) {
			continue
		}
		if c.config.Server && h.Type == wire.Initial && len(data) < 1200 {
			continue
		}
		c.expireEarlyKeys(now)
		space, ok := spaceFor(h.Type)
		if !ok || c.packetReadKeys(h.Type, space) == nil {
			continue
		}
		keys := c.packetReadKeys(h.Type, space)
		if err := c.checkIntegrityLimit(keys); err != nil && c.failedAuth >= c.integrityLimit {
			c.Close()
			return c.err
		}
		_, plain, _, openErr := keys.Open(raw, h.PNOffset, c.spaces[space].received.Expected())
		if errors.Is(openErr, packet.ErrAuthentication) {
			c.failedAuth++
			if c.failedAuth >= c.integrityLimit {
				c.Close()
				return c.err
			}
		}
		if openErr != nil {
			if authenticatedOpenError(openErr) {
				authenticated = true
				c.authenticatedPackets++
			}
			continue
		}
		authenticated = true
		c.authenticatedPackets++
		for len(plain) > 0 {
			frame, n, err := wire.ConsumeFrame(plain)
			if err != nil {
				break
			}
			ctx := wire.FrameContext{PacketType: h.Type, SenderIsServer: !c.config.Server, ResetStreamAtEnabled: c.config.Parameters.ResetStreamAt, MaxDatagramFrameSize: c.config.Parameters.MaxDatagramFrameSize}
			if wire.ValidateFrame(frame, ctx, n) != nil {
				break
			}
			plain = plain[n:]
			if _, ok := frame.(wire.ConnectionCloseFrame); ok {
				return c.beginDraining(ErrPeerClosed, now)
			}
		}
	}
	if !now.Before(c.closing.nextResponse) {
		c.closing.pending = true
	}
	return c.err
}
