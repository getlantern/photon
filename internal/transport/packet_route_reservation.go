package transport

import (
	"net/netip"
	"time"
)

// packetRouteReservations is scoped to one serialized packet-selection call.
// The controller supplies only authorized, publication-ready foreign routes;
// locals contains its owned sockets. No socket I/O or owner-to-owner call occurs
// during selection. Unused tickets must be canceled when selection ends.
type packetRouteReservations struct {
	locals  map[netip.AddrPort]bool
	foreign map[netip.AddrPort]foreignPacketRoute
	ticket  *foreignWriteReservation
	tuple   pathTuple
	err     error
}

type foreignPacketRoute struct {
	writer *packetWriter
	route  *handoffRoute
}

func (p *packetRouteReservations) reserve(tuple pathTuple) bool {
	// A previous candidate can reserve and then fail another construction gate.
	// Capacity belongs only to the packet ultimately selected, never that attempt.
	p.cancel()
	p.err = nil
	if p.locals[tuple.local] {
		return true
	}
	selected, ok := p.foreign[tuple.local]
	if !ok || selected.writer == nil || selected.route == nil || selected.route.local != tuple.local {
		p.err = errPathBinding
		return false
	}
	ticket, err := selected.writer.reserve(selected.route)
	if err != nil {
		p.err = err
		return false
	}
	p.ticket, p.tuple = ticket, tuple
	return true
}
func (p *packetRouteReservations) cancel() {
	if p.ticket != nil {
		p.ticket.cancel()
		p.ticket = nil
	}
}

// take transfers the selected packet's ticket to its write completion owner.
// Empty output and construction errors must instead call cancel.
func (p *packetRouteReservations) take(tuple pathTuple) (*foreignWriteReservation, error) {
	if p.ticket == nil {
		if p.locals[tuple.local] {
			return nil, nil
		}
		return nil, errPathBinding
	}
	if p.tuple != tuple {
		p.cancel()
		return nil, errPathBinding
	}
	ticket := p.ticket
	p.ticket = nil
	return ticket, nil
}

// nextBoundPacketWithRoutes keeps reservations and borrowed gate state scoped to
// one selection. The returned ticket, if nonnil, must be submitted or canceled.
func (c *Conn) nextBoundPacketWithRoutes(now time.Time, routes *packetRouteReservations) (uint64, pathTuple, []byte, *foreignWriteReservation, error) {
	if c.packetRoutes != nil || routes == nil {
		return 0, pathTuple{}, nil, nil, errPathBinding
	}
	c.packetRoutes = routes
	defer func() { c.packetRoutes = nil; routes.cancel() }()
	id, tuple, data, err := c.nextBoundMultipathPacket(now)
	if err != nil || len(data) == 0 {
		return id, tuple, data, nil, err
	}
	ticket, err := routes.take(tuple)
	return id, tuple, data, ticket, err
}
