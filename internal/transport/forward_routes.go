package transport

import (
	"context"
	"errors"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/getlantern/photon/internal/wire"
)

const maxForwardCIDs = 4096

// handoffRoute holds source-owned CID reservations and a mutex-protected update
// request, separate from Conn state. router/target/wake/local are immutable. Replacing
// the CID set is atomic with respect to dispatch and local CID issuance. Callers
// must confirm installation before advertising IDs.
type handoffRoute struct {
	resetHints atomic.Pointer[forwardResetSnapshot]
	updateMu   sync.Mutex
	pending    *handoffCIDUpdate
	revoked    atomic.Bool
	wake       chan<- struct{}
	router     *Router
	target     *packetHandoff
	local      netip.AddrPort // immutable source socket identity
	ids        []string
}

func (route *handoffRoute) revoke() {
	route.revoked.Store(true)
	route.finishCIDUpdate(nil, ErrEndpointClosed)
	route.router.forwardDirty.Store(true)
	select {
	case route.wake <- struct{}{}:
	default:
	}
}

func cidPrefixesOverlap(a, b string) bool { return strings.HasPrefix(a, b) || strings.HasPrefix(b, a) }

func (r *Router) setForwardCIDs(route *handoffRoute, ids [][]byte) error {
	if route == nil || route.router != r || route.target == nil {
		return errors.New("quic: invalid forwarding registration")
	}
	r.pruneForwardRoutes()
	if route.revoked.Load() {
		return ErrEndpointClosed
	}
	if len(ids) > maxForwardCIDs {
		return ErrRouterCapacity
	}
	if len(ids) > 0 {
		for _, local := range r.entries {
			if local.zero {
				return ErrRouteCollision
			}
		}
	}
	desired := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if len(id) == 0 || len(id) > 20 {
			return errors.New("quic: invalid forwarding CID")
		}
		desired[string(id)] = struct{}{}
	}
	if len(r.foreignIDs)-len(route.ids)+len(desired) > maxForwardCIDs {
		return ErrRouterCapacity
	}
	for id := range desired {
		for local := range r.ids {
			if cidPrefixesOverlap(id, local) {
				return ErrRouteCollision
			}
		}
		for foreign, owner := range r.foreignIDs {
			if owner != route && cidPrefixesOverlap(id, foreign) {
				return ErrRouteCollision
			}
		}
		for other := range desired {
			if other != id && cidPrefixesOverlap(id, other) {
				return ErrRouteCollision
			}
		}
	}
	for _, id := range route.ids {
		delete(r.foreignIDs, id)
	}
	route.ids = nil
	if len(desired) > 0 && r.foreignIDs == nil {
		r.foreignIDs = make(map[string]*handoffRoute)
	}
	for id := range desired {
		r.foreignIDs[id] = route
		route.ids = append(route.ids, id)
	}
	return nil
}

// forwardTarget handles only short-header packets: path registration is for
// established connections. Greased QUIC bits remain routable. Prefix-free CID
// reservations permit different CID lengths without ambiguous packet dispatch.
func (r *Router) forwardTarget(data []byte) *packetHandoff {
	if route := r.forwardRoute(data); route != nil {
		return route.target
	}
	return nil
}

func (r *Router) forwardRoute(data []byte) *handoffRoute {
	if len(r.foreignIDs) == 0 || len(data) < 2 {
		return nil
	}
	if data[0]&0x80 != 0 {
		// Long-header CID dispatch preserves authentication precedence for a
		// coalesced datagram. A malformed header can still use reset hints.
		h, err := wire.ParseHeaderWithQUICBit(data, 0, true)
		if err != nil && !errors.Is(err, wire.ErrVersion) {
			return nil
		}
		return r.foreignIDs[string(h.DCID)]
	}
	for n := 1; n <= 20 && n < len(data); n++ {
		if route := r.foreignIDs[string(data[1:1+n])]; route != nil {
			return route
		}
	}
	return nil
}

// registerHandoffRoute and updateHandoffRoute must be called outside owner
// callbacks. Bound routes use automatic publication instead of manual updates.
func (s *UDPServer) registerHandoffRoute(ctx context.Context, target *packetHandoff, ids [][]byte) (*handoffRoute, error) {
	var route *handoffRoute
	err := s.Do(ctx, func(e *ServerEndpoint) error {
		if target == nil || target == s.handoff {
			return errors.New("quic: invalid forwarding target")
		}
		e.router.pruneForwardRoutes()
		if len(e.router.forwardRegistrations) >= maxForwardCIDs {
			return ErrRouterCapacity
		}
		if s.routesChanged == nil {
			s.routesChanged = make(chan struct{}, 1)
		}
		route = &handoffRoute{router: e.router, target: target, wake: s.routesChanged, local: s.primaryAddress}
		if !target.subscribe(route) {
			return ErrEndpointClosed
		}
		if err := e.router.setForwardCIDs(route, ids); err != nil {
			target.unsubscribe(route)
			return err
		}
		if e.router.forwardRegistrations == nil {
			e.router.forwardRegistrations = make(map[*handoffRoute]struct{})
		}
		e.router.forwardRegistrations[route] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return route, nil
}
func (s *UDPServer) updateHandoffRoute(ctx context.Context, route *handoffRoute, ids [][]byte) error {
	return s.Do(ctx, func(e *ServerEndpoint) error { return e.router.setForwardCIDs(route, ids) })
}

// pruneForwardRoutes runs only on the source owner. A destination closing merely
// marks registrations and posts a coalesced wakeup; it never edits source maps.
func (r *Router) pruneForwardRoutes() {
	if len(r.forwardRegistrations) == 0 || !r.forwardDirty.Swap(false) {
		return
	}
	for route := range r.forwardRegistrations {
		if route.revoked.Load() {
			r.removeForwardRoute(route)
		}
	}
}
func (r *Router) removeForwardRoute(route *handoffRoute) {
	route.target.unsubscribe(route)
	for _, id := range route.ids {
		delete(r.foreignIDs, id)
	}
	route.ids = nil
	delete(r.forwardRegistrations, route)
}
func (s *UDPServer) closeHandoffRoute(ctx context.Context, route *handoffRoute) error {
	return s.Do(ctx, func(e *ServerEndpoint) error {
		if route == nil || route.router != e.router {
			return errors.New("quic: invalid forwarding registration")
		}
		e.router.removeForwardRoute(route)
		return nil
	})
}
