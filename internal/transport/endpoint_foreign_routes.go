package transport

import "net/netip"

// bindForeignWriter runs on the connection owner after the source registration
// has been bound to this connection. Binding does not establish publication
// readiness or validate a network path, and does not make the socket a listener.
func (e *ServerEndpoint) bindForeignWriter(c *Conn, writer *packetWriter, route *handoffRoute) error {
	if e.closed || c == nil || c.closed || c.Closing() || c.Draining() {
		return ErrEndpointClosed
	}
	if _, ok := e.router.entries[c]; !ok {
		return errPathBinding
	}
	if writer == nil || route == nil || writer.router != route.router || route.target == nil || route.target != e.router.handoff || !route.local.IsValid() || route.local.Port() == 0 || route.local.Addr().IsUnspecified() || canonicalPeer(route.local) != route.local || e.locals[route.local] {
		return errPathBinding
	}
	if !c.confirmed || c.multipath == nil || e.networks[c] == nil {
		return ErrHandshakeUnconfirmed
	}
	if !foreignWriterLive(writer, route) {
		return ErrEndpointClosed
	}
	h := route.target
	h.mu.Lock()
	bound := !h.closed && h.routes[route] == c && h.connections[c][route] != nil
	h.mu.Unlock()
	if !bound {
		return errPathBinding
	}
	e.pruneForeignWriters(c)
	routes := e.foreignWriters[c]
	if existing, ok := routes[route.local]; ok {
		if existing.writer == writer && existing.route == route {
			return nil
		}
		return ErrRouteCollision
	}
	if e.foreignWriterCount >= maxForwardCIDs {
		for connection := range e.foreignWriters {
			e.pruneForeignWriters(connection)
		}
	}
	if e.foreignWriterCount >= maxForwardCIDs {
		return ErrRouterCapacity
	}
	if e.foreignWriters == nil {
		e.foreignWriters = make(map[*Conn]map[netip.AddrPort]foreignPacketRoute)
	}
	routes = e.foreignWriters[c]
	if routes == nil {
		routes = make(map[netip.AddrPort]foreignPacketRoute)
		e.foreignWriters[c] = routes
	}
	routes[route.local] = foreignPacketRoute{writer, route}
	e.foreignWriterCount++
	return nil
}

func foreignWriterLive(writer *packetWriter, route *handoffRoute) bool {
	if route.revoked.Load() {
		return false
	}
	writer.mu.Lock()
	live := !writer.closed
	writer.mu.Unlock()
	return live
}

func (e *ServerEndpoint) removeForeignWriter(c *Conn, local netip.AddrPort) {
	routes := e.foreignWriters[c]
	binding, ok := routes[local]
	if !ok {
		return
	}
	delete(routes, local)
	e.foreignWriterCount--
	if len(routes) == 0 {
		delete(e.foreignWriters, c)
	}
	binding.route.target.unsubscribe(binding.route)
}
func (e *ServerEndpoint) pruneForeignWriters(c *Conn) {
	for local, binding := range e.foreignWriters[c] {
		if !foreignWriterLive(binding.writer, binding.route) {
			e.removeForeignWriter(c, local)
		}
	}
}

// foreignPacketRoutes returns a per-selection snapshot. Only the exact bound
// registration's acknowledgement of the current CID revision authorizes output.
// A pending or failed alternative remains absent without closing the primary.
// Source closure/revocation is rechecked again by reservation and socket write.
func (e *ServerEndpoint) foreignPacketRoutes(c *Conn) *packetRouteReservations {
	scope := &packetRouteReservations{locals: e.locals}
	if e.closed || c == nil || c.closed {
		return scope
	}
	e.pruneForeignWriters(c)
	_, err := e.refreshForwardPublication(c)
	if err != nil || len(e.foreignWriters[c]) == 0 {
		return scope
	}
	snapshot, ok := e.router.entries[c]
	if !ok || snapshot.zero {
		return scope
	}
	h := e.router.handoff
	if h == nil {
		return scope
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return scope
	}
	for local, binding := range e.foreignWriters[c] {
		route := binding.route
		publication := h.connections[c][route]
		if route.target != h || route.revoked.Load() || h.routes[route] != c || publication == nil || publication.err != nil || publication.result != nil || publication.installed != snapshot.revision {
			continue
		}
		if scope.foreign == nil {
			scope.foreign = make(map[netip.AddrPort]foreignPacketRoute)
		}
		scope.foreign[local] = binding
	}
	return scope
}

// A registered foreign socket can keep a candidate alive before publication is
// ready. Output authorization remains the stricter per-selection snapshot.
func (e *ServerEndpoint) localSocketAvailable(c *Conn, local netip.AddrPort) bool {
	if e.locals[local] {
		return true
	}
	binding, ok := e.foreignWriters[c][local]
	return ok && foreignWriterLive(binding.writer, binding.route)
}
