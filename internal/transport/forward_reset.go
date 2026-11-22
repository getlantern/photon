package transport

import (
	"crypto/subtle"
	"net/netip"
)

// forwardResetSnapshot is an immutable routing hint published by the connection
// owner. The source never reads Conn state. The destination must recheck current
// tuple/token eligibility and attempt authentication before accepting a reset.
type forwardResetSnapshot struct {
	peers  map[netip.AddrPort]struct{}
	tokens map[[16]byte]struct{}
	// Owner-side comparison state retains path and CID identities. Counting raw
	// visits against deduplicated set sizes cannot detect unchanged shared peers
	// and can miss a removal when another path starts using the same value.
	paths map[uint64]forwardResetPathSnapshot
}

type forwardResetPathSnapshot struct {
	bound, candidate netip.AddrPort
	tokens           map[uint64][16]byte
}

func resetHintPeers(c *Conn, state *pathCIDState, local netip.AddrPort) (bound, candidate netip.AddrPort) {
	if app := state.application; app != nil && app.network != nil && app.network.local == local {
		bound = app.network.remote
	}
	if !c.config.Server {
		if attempt := state.resetCandidate(); attempt != nil && attempt.tuple.local == local {
			candidate = attempt.tuple.remote
		}
	}
	return
}

// matches runs only on the connection owner. It compares identities as well as
// values, so repeated peers/tokens across paths neither allocate nor hide removal.
func (snapshot *forwardResetSnapshot) matches(c *Conn, local netip.AddrPort) bool {
	if snapshot == nil {
		return false
	}
	if c.multipath == nil {
		return len(snapshot.paths) == 0 && len(snapshot.peers) == 0 && len(snapshot.tokens) == 0
	}
	paths := 0
	for id, state := range c.multipath.paths {
		if c.multipath.abandoned[id] != nil {
			continue
		}
		paths++
		old, ok := snapshot.paths[id]
		if !ok {
			return false
		}
		bound, candidate := resetHintPeers(c, state, local)
		if bound != old.bound || candidate != old.candidate {
			return false
		}
		tokens := 0
		for sequence, peer := range state.peer.active {
			if !peer.used || !peer.hasToken {
				continue
			}
			tokens++
			if token, ok := old.tokens[sequence]; !ok || token != peer.token {
				return false
			}
		}
		if tokens != len(old.tokens) {
			return false
		}
	}
	return paths == len(snapshot.paths)
}

func (route *handoffRoute) publishResetHints(c *Conn) {
	if route.resetHints.Load().matches(c, route.local) {
		return
	}
	next := &forwardResetSnapshot{}
	if c.multipath != nil {
		next.paths = make(map[uint64]forwardResetPathSnapshot, len(c.multipath.paths))
		next.peers = make(map[netip.AddrPort]struct{}, len(c.multipath.paths))
		next.tokens = make(map[[16]byte]struct{}, len(c.multipath.paths))
		for id, state := range c.multipath.paths {
			if c.multipath.abandoned[id] != nil {
				continue
			}
			bound, candidate := resetHintPeers(c, state, route.local)
			path := forwardResetPathSnapshot{bound: bound, candidate: candidate}
			if bound.IsValid() {
				next.peers[bound] = struct{}{}
			}
			if candidate.IsValid() {
				next.peers[candidate] = struct{}{}
			}
			for sequence, peer := range state.peer.active {
				// Foreign output publishes first-use tokens before exposing data
				// to the socket. Unused and retired tokens remain ineligible.
				if !peer.used || !peer.hasToken {
					continue
				}
				if path.tokens == nil {
					path.tokens = make(map[uint64][16]byte)
				}
				path.tokens[sequence] = peer.token
				next.tokens[peer.token] = struct{}{}
			}
			next.paths[id] = path
		}
	}
	route.resetHints.Store(next)
}

// forwardResetRoute is only a fallback for otherwise unroutable packets.
// Exact tuple and token hints avoid broadcasting unauthenticated input to owners.
func (r *Router) forwardResetRoute(data []byte, local, peer netip.AddrPort) *handoffRoute {
	if len(data) < 21 || len(r.forwardRegistrations) == 0 {
		return nil
	}
	if _, known := r.RouteNetwork(data, local, peer); known {
		return nil
	}
	return r.foreignResetMatch(data, local, peer)
}

// foreignResetMatch can also run after the local CID owner fails authentication.
// It carries no authority to bypass final tuple/token checks at the destination.
func (r *Router) foreignResetMatch(data []byte, local, peer netip.AddrPort) *handoffRoute {
	route, ambiguous := r.foreignResetSelection(data, local, peer)
	if ambiguous {
		return nil
	}
	return route
}

func (r *Router) foreignResetSelection(data []byte, local, peer netip.AddrPort) (*handoffRoute, bool) {
	if len(data) < 21 || len(r.forwardRegistrations) == 0 {
		return nil, false
	}
	tuple, err := canonicalPathTuple(local, peer)
	if err != nil {
		return nil, false
	}
	var token [16]byte
	copy(token[:], data[len(data)-16:])
	var found *handoffRoute
	for route := range r.forwardRegistrations {
		if route.local != tuple.local || route.revoked.Load() {
			continue
		}
		hint := route.resetHints.Load()
		if hint == nil {
			continue
		}
		if _, ok := hint.peers[tuple.remote]; !ok {
			continue
		}
		match := 0
		for expected := range hint.tokens {
			match |= subtle.ConstantTimeCompare(token[:], expected[:])
		}
		if match == 0 {
			continue
		}
		// Ambiguous token publication cannot select an arbitrary connection.
		if found != nil {
			return nil, true
		}
		found = route
	}
	return found, false
}
