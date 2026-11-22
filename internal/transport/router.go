package transport

import (
	"errors"
	"net/netip"
	"sync/atomic"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

var ErrRouteCollision = errors.New("quic: connection routing identity collision")
var ErrRouterCapacity = errors.New("quic: connection router capacity reached")

type routeCIDState struct {
	local       *localIDs
	highest     uint64
	activeCount int
}
type connectionRoute struct {
	revision    uint64
	multipath   *pathCIDs
	paths       map[uint64]routeCIDState
	peer        netip.AddrPort
	ids         []string
	zero        bool
	local       *localIDs
	highest     uint64
	activeCount int
	synced      bool
}

// Router demultiplexes one local UDP endpoint. It is serialized by its owner,
// like Conn. Sync must follow connection events and precede transmission of
// newly issued CIDs. CID collisions must prevent transmission, not be ignored.
// This initial router enforces fixed peer addresses; migration is not enabled.
type Router struct {
	resetIndex           *resetTokenIndex
	cidLength, capacity  int
	ids                  map[string]*Conn
	foreignIDs           map[string]*handoffRoute
	forwardRegistrations map[*handoffRoute]struct{}
	forwardDirty         atomic.Bool
	forwardPendingIDs    atomic.Int64
	handoff              *packetHandoff
	peers                map[netip.AddrPort]map[*Conn]struct{}
	entries              map[*Conn]connectionRoute
}

func NewRouter(cidLength, capacity int) (*Router, error) {
	if cidLength < 0 || cidLength > 20 || capacity < 1 || capacity > 65536 {
		return nil, errors.New("quic: invalid router limits")
	}
	return &Router{cidLength: cidLength, capacity: capacity, ids: make(map[string]*Conn), peers: make(map[netip.AddrPort]map[*Conn]struct{}), entries: make(map[*Conn]connectionRoute)}, nil
}
func canonicalPeer(peer netip.AddrPort) netip.AddrPort {
	return netip.AddrPortFrom(peer.Addr().Unmap(), peer.Port())
}

// Add registers routing before the first output packet is handed to the socket.
func (r *Router) Add(c *Conn, peer netip.AddrPort) error {
	if c == nil || c.closed || !peer.IsValid() || peer.Port() == 0 {
		return errors.New("quic: invalid connection route")
	}
	if _, ok := r.entries[c]; ok {
		return ErrRouteCollision
	}
	if len(r.entries) >= r.capacity {
		return ErrRouterCapacity
	}
	if len(c.config.LocalCID) != 0 && len(c.config.LocalCID) != r.cidLength {
		return errors.New("quic: inconsistent local CID length")
	}
	peer = canonicalPeer(peer)
	r.entries[c] = connectionRoute{peer: peer}
	if err := r.Sync(c); err != nil {
		delete(r.entries, c)
		return err
	}
	if r.peers[peer] == nil {
		r.peers[peer] = make(map[*Conn]struct{})
	}
	r.peers[peer][c] = struct{}{}
	return nil
}

// Sync atomically reflects CID issuance and retirement. Closing/draining IDs
// remain registered until the connection becomes closed. The original Initial
// routing alias remains for the connection's lifetime to prevent readmission.
func (r *Router) Sync(c *Conn) (result error) {
	defer func() {
		if result == nil && r.resetIndex != nil {
			if _, registered := r.entries[c]; registered {
				r.resetIndex.sync(c)
			}
		}
	}()
	r.pruneForwardRoutes()
	route, ok := r.entries[c]
	if !ok {
		return errors.New("quic: unregistered connection")
	}
	if c.closed {
		r.Remove(c)
		return nil
	}
	var highest uint64
	var count int
	if c.localIDs != nil {
		highest = c.localIDs.highest
		count = len(c.localIDs.active)
	}
	// Local CID values are immutable. Issuance increases highest, retirement
	// decreases count, and replacing the owner changes its pointer. Marking an
	// issued CID sent does not change its routing reservation. Config's Initial
	// alias and the registered peer address are immutable after registration.
	if c.multipath == nil && route.multipath == nil && route.synced && route.local == c.localIDs && route.highest == highest && route.activeCount == count {
		return nil
	}
	// A multipath route has the same immutable CID invariant, independently
	// for every local-ID owner. Snapshot membership also detects path collection
	// and replacement even when the number of paths does not change.
	if c.multipath != nil && route.synced && route.multipath == c.multipath && len(route.paths) == len(c.multipath.paths) {
		unchanged := true
		for id, state := range c.multipath.paths {
			saved, ok := route.paths[id]
			if !ok || saved.local != state.local || saved.highest != state.local.highest || saved.activeCount != len(state.local.active) {
				unchanged = false
				break
			}
		}
		if unchanged {
			return nil
		}
	}
	desired := make(map[string]struct{})
	if c.multipath != nil {
		for _, state := range c.multipath.paths {
			for _, id := range state.local.active {
				desired[string(id.cid)] = struct{}{}
			}
		}
	} else if c.localIDs != nil {
		for _, id := range c.localIDs.active {
			desired[string(id.cid)] = struct{}{}
		}
	}
	if c.config.Server {
		initial := c.config.OriginalDCID
		if c.config.RetrySourceCID != nil {
			initial = c.config.RetrySourceCID
		}
		desired[string(initial)] = struct{}{}
	}
	_, zero := desired[""]
	if zero && len(r.foreignIDs) > 0 {
		return ErrRouteCollision
	}
	for other := range r.peers[route.peer] {
		if other != c && (zero || r.entries[other].zero) {
			return ErrRouteCollision
		}
	}
	for id := range desired {
		if id != "" {
			for foreign := range r.foreignIDs {
				if cidPrefixesOverlap(id, foreign) {
					return ErrRouteCollision
				}
			}
			if owner := r.ids[id]; owner != nil && owner != c {
				return ErrRouteCollision
			}
		}
	}
	if route.revision == ^uint64(0) {
		return ErrRouterCapacity
	}
	for _, id := range route.ids {
		delete(r.ids, id)
	}
	route.revision++
	route.ids = nil
	route.zero = zero
	route.local, route.highest, route.activeCount, route.synced = c.localIDs, highest, count, true
	for id := range desired {
		if id != "" {
			r.ids[id] = c
			route.ids = append(route.ids, id)
		}
	}
	// Publish snapshots only after collision preflight and route commit.
	route.multipath = c.multipath
	route.paths = nil
	if c.multipath != nil {
		route.paths = make(map[uint64]routeCIDState, len(c.multipath.paths))
		for id, state := range c.multipath.paths {
			route.paths[id] = routeCIDState{local: state.local, highest: state.local.highest, activeCount: len(state.local.active)}
		}
	}
	r.entries[c] = route
	return nil
}
func (r *Router) Remove(c *Conn) {
	if r.resetIndex != nil {
		r.resetIndex.remove(c)
	}
	route, ok := r.entries[c]
	if !ok {
		return
	}
	if r.handoff != nil {
		r.handoff.revokeConnection(c)
	}
	for _, id := range route.ids {
		delete(r.ids, id)
	}
	delete(r.peers[route.peer], c)
	if len(r.peers[route.peer]) == 0 {
		delete(r.peers, route.peer)
	}
	delete(r.entries, c)
}

// Route returns a connection and whether the datagram belongs to a reserved
// routing identity. A known CID from another address returns (nil,true): discard
// it rather than accidentally admitting a second connection on the same CID.
// Unknown datagrams return (nil,false), allowing stateless Initial admission.
// CID lookup tolerates either QUIC bit; the selected connection applies its
// local receive policy before authentication or frame processing.
func (r *Router) Route(data []byte, peer netip.AddrPort) (*Conn, bool) {
	if !peer.IsValid() || peer.Port() == 0 {
		return nil, false
	}
	return r.routeDatagram(data, pathTuple{remote: canonicalPeer(peer)}, false)
}

// RouteNetwork is for address-aware owners. A multipath CID selects only the
// connection, not address-validation authority: networkOwner must authenticate
// and enforce each path's tuple before applying frames. Legacy connections keep
// Route's fixed-peer policy. Unroutable resets are restricted to known tuples.
func (r *Router) RouteNetwork(data []byte, local, peer netip.AddrPort) (*Conn, bool) {
	tuple, err := canonicalPathTuple(local, peer)
	if err != nil {
		return nil, false
	}
	return r.routeDatagram(data, tuple, true)
}

func (r *Router) routeDatagram(data []byte, tuple pathTuple, network bool) (*Conn, bool) {
	if c, known := r.routeIdentity(data, tuple, network); known {
		return c, true
	}
	if len(data) < 21 {
		return nil, false
	}
	var found *Conn
	consider := func(c *Conn) bool {
		route, registered := r.entries[c]
		if !registered || c.closed || c.Draining() {
			return false
		}
		if network && c.multipath != nil {
			if !c.resetTupleKnown(tuple, time.Time{}) {
				return false
			}
		} else if route.peer != tuple.remote {
			return false
		}
		if !c.isStatelessReset(data) {
			return false
		}
		if found != nil {
			return true
		}
		found = c
		return false
	}
	// Ambiguity reserves the packet without selecting a connection or allowing
	// listener admission. The endpoint rechecks live eligibility before reception.
	// A single registered owner is cheaper to check directly than to hash.
	if r.resetIndex != nil && len(r.entries) > 1 {
		for c := range r.resetIndex.candidates(data) {
			if consider(c) {
				return nil, true
			}
		}
	} else if network {
		for c := range r.entries {
			if consider(c) {
				return nil, true
			}
		}
	} else {
		for c := range r.peers[tuple.remote] {
			if consider(c) {
				return nil, true
			}
		}
	}
	return found, found != nil
}

// routeIdentity excludes reset-token hints so callers can resolve ambiguity
// across local and foreign ownership before reset reception has side effects.
func (r *Router) routeIdentity(data []byte, tuple pathTuple, network bool) (*Conn, bool) {
	h, err := wire.ParseHeaderWithQUICBit(data, r.cidLength, true)
	if err == nil || errors.Is(err, wire.ErrVersion) {
		if c := r.ids[string(h.DCID)]; c != nil {
			if !(network && c.multipath != nil) && r.entries[c].peer != tuple.remote {
				return nil, true
			}
			return c, true
		}
	}
	// Zero-length CID routing, like an explicit CID, takes precedence over token
	// hints so authenticated traffic cannot be diverted by its random suffix.
	// Sync forbids sharing a peer bucket with a zero-length CID owner. A
	// multi-entry bucket therefore needs no zero-CID scan.
	if peers := r.peers[tuple.remote]; len(peers) == 1 {
		for c := range peers {
			if r.entries[c].zero && len(data) > 0 && (data[0]&0x80 == 0 || (err == nil && len(h.DCID) == 0)) {
				return c, true
			}
		}
	}
	return nil, false
}
