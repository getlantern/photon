package transport

import (
	"errors"
	"syscall"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

type outboundMTUProbe struct {
	idleBefore     idleActivity
	idleRevision   uint64
	pathID, number uint64
	app            *pathApplication
	size           int
}

func (c *Conn) outboundMTUProbe(id uint64, size int) *outboundMTUProbe {
	if c.multipath == nil || c.Closing() {
		return nil
	}
	state := c.multipath.paths[id]
	if state == nil || state.application == nil {
		return nil
	}
	app := state.application
	m := app.mtu
	if m == nil || !m.pending || m.candidate != size || app.space.nextPN != m.lastNumber+1 {
		return nil
	}
	return &outboundMTUProbe{idleRevision: c.idleRevision, pathID: id, number: m.lastNumber, app: app, size: size}
}

// HandleWriteError consumes an atomic zero-byte failed write of a committed
// datagram. Asynchronous callers must retain the descriptor and prevent further
// packet construction until this result has been handled.
// Dedicated MTU probes rejected with EMSGSIZE narrow their search. Oversized
// STREAM packets fall back to baseline validation and smaller retransmissions.
// Oversized DATAGRAM packets are dropped and trigger the same baseline fallback.
// Other failures retain connection removal.
// Calls must be serialized with NextDatagram and receive processing.
func (e *ServerEndpoint) HandleWriteError(out *Outbound, err error, now time.Time) {
	if out == nil || err == nil {
		return
	}
	if probe := out.mtu; probe != nil && out.Conn != nil && errors.Is(err, syscall.EMSGSIZE) && len(out.Data) == probe.size {
		app := probe.app
		m := app.mtu
		if m != nil && m.pending && m.lastNumber == probe.number && m.candidate == probe.size && app.sent.RejectMTUProbe(probe.number) {
			m.tooLarge(probe.number)
			app.updateMTURaise(now)
			out.Conn.restoreIdleActivity(probe.idleBefore, probe.idleRevision)
			out.Conn.armPathRecovery(probe.pathID, now)
			return
		}
	}
	if packet := out.large; packet != nil && out.Conn != nil && errors.Is(err, syscall.EMSGSIZE) && len(out.Data) == packet.size {
		app := packet.app
		if app.mtu != nil && app.space.sent[packet.number] == packet.item && app.sent.RejectLargePacket(packet.number) {
			queueLostItems(app.space, app.space, []recovery.Packet{{Number: packet.number}})
			delete(app.space.sent, packet.number)
			app.resetMTU()
			app.mtuNeedsValidation = true
			app.mtu.upper = minimumQUICMTU
			app.updateMTURaise(now)
			out.Conn.restoreIdleActivity(packet.idleBefore, packet.idleRevision)
			out.Conn.armPathRecovery(packet.pathID, now)
			return
		}
	}

	if packet := out.datagram; packet != nil && out.Conn != nil && errors.Is(err, syscall.EMSGSIZE) && len(out.Data) == packet.size {
		app := packet.app
		if app.mtu != nil && app.lastSentDatagram && app.space.nextPN == packet.number+1 && app.sent.RejectLargePacket(packet.number) {
			// Payload and queue accounting were already released at commitment.
			// DATAGRAM frames are never retransmitted after write rejection.
			app.resetMTU()
			app.mtuNeedsValidation = true
			app.mtu.upper = minimumQUICMTU
			app.updateMTURaise(now)
			out.Conn.restoreIdleActivity(packet.idleBefore, packet.idleRevision)
			out.Conn.armPathRecovery(packet.pathID, now)
			return
		}
	}

	if packet := out.baseLarge; packet != nil && out.Conn != nil && errors.Is(err, syscall.EMSGSIZE) && len(out.Data) == packet.size {
		c := out.Conn
		s := &c.spaces[applicationSpace]
		if c.multipath == nil && s.nextPN == packet.number+1 && (packet.item == nil || s.sent[packet.number] == packet.item) && c.recovery.RejectLargePacket(packet.number) {
			if packet.item != nil {
				c.queueLost(applicationSpace, []recovery.Packet{{Number: packet.number}})
				delete(s.sent, packet.number)
			}
			if packet.ack {
				// The bundled ACK never left: owe it again, now. If it is
				// what marked an ACK sent in this key phase, unmark it; a
				// phase that has since moved on keeps its own state.
				s.ackPending = true
				if !packet.ackSent && c.applicationKeys.firstSent == packet.ackPhase {
					c.applicationKeys.ackSent = false
				}
			}
			c.basePacketLimit, c.baseMTUNeedsValidation = minimumQUICMTU, true
			c.baseMTUFallbackAt = now
			c.restoreIdleActivity(packet.idleBefore, packet.idleRevision)
			// An older explicit validation may have its challenge behind the
			// now-blocked application queue. Restart in the reserved slot.
			c.retirePathChallenges()
			c.validation = pathValidation{}
			if err := c.ValidatePath(); err != nil {
				e.Remove(c)
				return
			}
			c.armTimer(now)
			return
		}
	}

	if out.Conn != nil {
		e.Remove(out.Conn)
	}
}

// Only reliable STREAM data qualifies for automatic re-packetization.
type outboundLargeStream struct {
	outboundMTUProbe
	item *sendItem
}

func (c *Conn) outboundLargeStream(id uint64, size int) *outboundLargeStream {
	if size <= minimumQUICMTU || c.multipath == nil || c.Closing() {
		return nil
	}
	state := c.multipath.paths[id]
	if state == nil || state.application == nil {
		return nil
	}
	app := state.application
	if app.mtu == nil || app.space.nextPN == 0 {
		return nil
	}
	number := app.space.nextPN - 1
	item := app.space.sent[number]
	if item == nil {
		return nil
	}
	if _, ok := item.frame.(wire.StreamFrame); !ok {
		return nil
	}
	return &outboundLargeStream{outboundMTUProbe: outboundMTUProbe{idleRevision: c.idleRevision, pathID: id, number: number, app: app, size: size}, item: item}
}

func (c *Conn) outboundLargeDatagram(id uint64, size int) *outboundMTUProbe {
	if size <= minimumQUICMTU || c.multipath == nil || c.Closing() {
		return nil
	}
	state := c.multipath.paths[id]
	if state == nil || state.application == nil {
		return nil
	}
	app := state.application
	if app.mtu == nil || !app.lastSentDatagram || app.space.nextPN == 0 {
		return nil
	}
	return &outboundMTUProbe{idleRevision: c.idleRevision, pathID: id, number: app.space.nextPN - 1, app: app, size: size}
}

type outboundBaseLarge struct {
	idleBefore   idleActivity
	idleRevision uint64
	number       uint64
	size         int
	item         *sendItem // nil for unreliable DATAGRAM
	ack, ackSent bool      // a bundled ACK, and the key-phase ackSent before it
	ackPhase     uint64    // firstSent of the keys the packet used
}

func (c *Conn) outboundBaseLarge(size int, storage *outboundBaseLarge) *outboundBaseLarge {
	if size <= minimumQUICMTU || c.multipath != nil || !c.confirmed || c.Closing() || !c.lastBaseApplication {
		return nil
	}
	s := &c.spaces[applicationSpace]
	if s.nextPN == 0 {
		return nil
	}
	number := s.nextPN - 1
	item := s.sent[number]
	if item != nil {
		if _, ok := item.frame.(wire.StreamFrame); !ok {
			return nil
		}
	} else if !c.lastBaseDatagram {
		return nil
	}
	if storage == nil {
		storage = new(outboundBaseLarge)
	}
	*storage = outboundBaseLarge{idleRevision: c.idleRevision, number: number, size: size, item: item, ack: c.lastBaseACK, ackSent: c.lastBaseACKSent, ackPhase: c.lastBaseACKPhase}
	return storage
}
