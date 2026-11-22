package transport

import (
	"net/netip"
	"testing"
	"time"
)

// Compare the derived index with the complete live-state scan after arbitrary
// owner events. This checks candidate completeness as well as stale-hit safety.
func FuzzResetTokenIndexLifecycle(f *testing.F) {
	f.Add([]byte{0, 0, 0, 1, 0, 1, 0, 1, 1, 0, 0, 1, 2, 1, 0, 1})
	f.Add([]byte{0, 0, 0, 2, 3, 0, 0, 0, 4, 0, 0, 0, 7, 0, 0, 0, 8, 0, 0, 0})
	f.Add([]byte{0, 0, 0, 1, 5, 0, 0, 1, 6, 0, 0, 1, 9, 0, 0, 1, 9, 0, 0, 0, 11, 0, 0, 0})
	f.Fuzz(func(t *testing.T, events []byte) {
		if len(events) > 128 {
			events = events[:128]
		}
		locals := []netip.AddrPort{netip.MustParseAddrPort("192.0.2.1:443"), netip.MustParseAddrPort("192.0.2.1:445")}
		peers := []netip.AddrPort{netip.MustParseAddrPort("192.0.2.2:444"), netip.MustParseAddrPort("192.0.2.3:444")}
		r, _ := NewRouter(8, 4)
		e := &ServerEndpoint{router: r, networks: make(map[*Conn]*networkOwner)}
		conns := make([]*Conn, 4)
		for i := range conns {
			c := resetPublicationBenchmarkConn(locals[0], peers[0], 2)
			c.multipath.abandoned = make(map[uint64]*pathAbandonment)
			conns[i] = c
			r.entries[c] = connectionRoute{peer: peers[0]}
			e.networks[c] = &networkOwner{conn: c, primary: pathTuple{locals[0], peers[0]}, activationChecked: true}
		}
		r.enableResetIndex()
		index := r.resetIndex
		if index == nil {
			t.Fatal("index initialization failed")
		}
		now := time.Unix(100, 0)
		for offset := 0; offset+3 < len(events); offset += 4 {
			op := events[offset] % 13
			c := conns[int(events[offset+1])%len(conns)]
			path := uint64(events[offset+2] % 2)
			value := events[offset+3]
			sequence := uint64(value % 2)
			state := c.multipath.paths[path]
			if state == nil {
				tuple := pathTuple{locals[0], peers[0]}
				state = &pathCIDState{application: &pathApplication{network: &tuple}, peer: &peerIDs{active: make(map[uint64]peerID)}}
				c.multipath.paths[path] = state
			}
			entry := state.peer.active[sequence]
			switch op {
			case 0:
				entry.token, entry.used, entry.hasToken = [16]byte{value % 4}, true, true
				state.peer.active[sequence] = entry
			case 1:
				entry.used = false
				state.peer.active[sequence] = entry
			case 2:
				delete(state.peer.active, sequence)
			case 3:
				c.multipath.abandoned[path] = &pathAbandonment{}
			case 4:
				delete(c.multipath.abandoned, path)
			case 5:
				state.application.network.remote = peers[value%2]
			case 6:
				state.application.network.local = locals[value%2]
			case 7:
				delete(r.entries, c)
			case 8:
				r.entries[c] = connectionRoute{peer: peers[0]}
			case 9:
				c.closed = value%2 != 0
			case 10:
				entry.hasToken = value%2 != 0
				state.peer.active[sequence] = entry
			case 11:
				delete(c.multipath.paths, path)
			case 12:
				state.peer.active[sequence+2] = entry // Repeated token within one owner.
			}
			if _, registered := r.entries[c]; registered {
				index.sync(c)
			} else {
				index.remove(c)
			}
			queries := map[[16]byte]struct{}{{0}: {}, {1}: {}, {2}: {}, {3}: {}, {0xff}: {}}
			for _, owner := range conns {
				for _, pathState := range owner.multipath.paths {
					for _, id := range pathState.peer.active {
						queries[id.token] = struct{}{}
					}
				}
			}
			for token := range queries {
				raw := resetDatagram(token)
				raw[0] = []byte{0x40, 0x00, 0x80, 0xc0}[value%4]
				if value == 255 {
					raw = raw[:20]
				}
				candidates := index.candidates(raw)
				for _, owner := range conns {
					_, registered := r.entries[owner]
					expected := registered && !owner.closed && !owner.Draining() && owner.isStatelessReset(raw)
					_, present := candidates[owner]
					if present != expected {
						t.Fatalf("event %d op %d: candidate completeness mismatch", offset/4, op)
					}
				}
				for _, local := range locals {
					for _, peer := range peers {
						got, ambiguous := e.localResetMatch(raw, local, peer, now, nil)
						r.resetIndex = nil
						want, wantAmbiguous := e.localResetMatch(raw, local, peer, now, nil)
						r.resetIndex = index
						if got != want || ambiguous != wantAmbiguous {
							t.Fatalf("event %d op %d: indexed selection differs from full scan", offset/4, op)
						}
						routed, known := r.RouteNetwork(raw, local, peer)
						if routed != want || known != (want != nil || wantAmbiguous) {
							t.Fatalf("event %d op %d: unknown-CID routing differs from live selection", offset/4, op)
						}
					}
				}
			}
			for _, bucket := range index.tokens {
				if len(bucket) == 0 {
					t.Fatal("empty token bucket retained")
				}
			}
		}
		for _, c := range conns {
			index.remove(c)
			index.remove(c)
		}
		if len(index.tokens) != 0 || len(index.connections) != 0 {
			t.Fatal("removal leaked token or owner identity")
		}
	})
}
