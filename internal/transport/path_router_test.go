package transport

import (
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestMultipathRouterReservationAndRetention(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	local := netip.MustParseAddrPort("192.0.2.1:443")
	peer := netip.MustParseAddrPort("192.0.2.2:444")
	other := netip.MustParseAddrPort("192.0.2.3:445")
	r, _ := NewRouter(8, 4)
	if err := r.Add(c, peer); err != nil {
		t.Fatal(err)
	}
	if err := c.advertisePathCIDs(2); err != nil {
		t.Fatal(err)
	}
	if err := r.Sync(c); err != nil {
		t.Fatal(err)
	}
	var cid []byte
	for _, id := range c.multipath.paths[2].local.active {
		cid = id.cid
		break
	}
	if _, ok := c.multipath.lookupLocal(cid); ok {
		t.Fatal("unsent CID active in engine")
	}
	// Router reserves CID collisions before advertisement; engine still rejects
	// unsent CIDs until packet construction activates their route.
	raw := append([]byte{0x43}, cid...)
	raw = append(raw, make([]byte, 24)...)
	if got, known := r.RouteNetwork(raw, local, other); got != c || !known {
		t.Fatal("missing reservation")
	}
	if got, known := r.Route(raw, other); got != nil || !known {
		t.Fatal("legacy peer check bypassed")
	}
	now := time.Unix(101, 0)
	if _, err := c.multipath.beginAbandon(2); err != nil {
		t.Fatal(err)
	}
	if _, err := c.multipath.receiveAbandon(wire.PathAbandonFrame{PathID: 2}, now, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := r.Sync(c); err != nil {
		t.Fatal(err)
	}
	if got, _ := r.RouteNetwork(raw, local, other); got != c {
		t.Fatal("retained CID reservation disappeared")
	}
	c.multipath.collectAbandoned(now.Add(3 * time.Second))
	if err := r.Sync(c); err != nil {
		t.Fatal(err)
	}
	if got, known := r.RouteNetwork(raw, local, other); got != nil || known {
		t.Fatal("collected CID still registered")
	}
}

func TestMultipathRouterCollisionAtomic(t *testing.T) {
	first, second := sendingPathPair(t, wire.Version1)
	r, _ := NewRouter(8, 4)
	a := netip.MustParseAddrPort("192.0.2.1:443")
	b := netip.MustParseAddrPort("192.0.2.2:443")
	if err := r.Add(first, a); err != nil {
		t.Fatal(err)
	}
	if err := r.Add(second, b); err != nil {
		t.Fatal(err)
	}
	var collision peerID
	for _, id := range first.multipath.paths[1].local.active {
		collision = id
		break
	}
	second.multipath.paths[1].local.active[99] = collision
	if err := r.Sync(second); !errors.Is(err, ErrRouteCollision) {
		t.Fatal(err)
	}
	if r.ids[string(collision.cid)] != first || r.ids[string(second.config.LocalCID)] != second {
		t.Fatal("collision corrupted existing routes")
	}
}

func TestMultipathRouterSecondaryReset(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version2)
	local := netip.MustParseAddrPort("192.0.2.1:443")
	primary := netip.MustParseAddrPort("192.0.2.2:444")
	secondary := netip.MustParseAddrPort("192.0.2.3:445")
	if err := c.bindPreparedPath(1, local, secondary); err != nil {
		t.Fatal(err)
	}
	state := c.multipath.paths[1]
	id := state.peer.active[state.peer.current]
	id.used = true
	state.peer.active[state.peer.current] = id
	r, _ := NewRouter(8, 4)
	if err := r.Add(c, primary); err != nil {
		t.Fatal(err)
	}
	reset := append([]byte{0xc0, 0, 0, 0, 1}, id.token[:]...)
	if got, known := r.RouteNetwork(reset, local, secondary); got != c || !known {
		t.Fatal("secondary reset not routed")
	}
	otherLocal := netip.MustParseAddrPort("192.0.2.4:443")
	if got, known := r.RouteNetwork(reset, otherLocal, secondary); got != nil || known {
		t.Fatal("unknown tuple reset routed")
	}
}

func TestMultipathRouterStableSyncAllocations(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	r, _ := NewRouter(8, 4)
	if err := r.Add(c, netip.MustParseAddrPort("192.0.2.1:443")); err != nil {
		t.Fatal(err)
	}
	allocations := testing.AllocsPerRun(100, func() {
		if err := r.Sync(c); err != nil {
			t.Fatal(err)
		}
	})
	if allocations != 0 {
		t.Fatalf("unchanged routing allocated %g objects", allocations)
	}
	// New CID issuance and retirement can preserve count; highest must invalidate.
	local := c.multipath.paths[1].local
	var removed string
	for seq, cid := range local.active {
		removed = string(cid.cid)
		delete(local.active, seq)
		break
	}
	if _, err := local.issue(2); err != nil {
		t.Fatal(err)
	}
	if err := r.Sync(c); err != nil {
		t.Fatal(err)
	}
	if r.ids[removed] != nil {
		t.Fatal("stale CID after same-count replacement")
	}
	for _, cid := range local.active {
		if r.ids[string(cid.cid)] != c {
			t.Fatal("missing replacement CID")
		}
	}
	// Replacing the local owner must invalidate even with equal counters.
	replacement := *local
	c.multipath.paths[1].local = &replacement
	if err := r.Sync(c); err != nil {
		t.Fatal(err)
	}
	if r.entries[c].paths[1].local != &replacement {
		t.Fatal("owner replacement not synchronized")
	}
}

func TestMultipathRouterSameCountPathReplacement(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	r, _ := NewRouter(8, 4)
	if err := r.Add(c, netip.MustParseAddrPort("192.0.2.1:443")); err != nil {
		t.Fatal(err)
	}
	var retired []string
	for _, cid := range c.multipath.paths[1].local.active {
		retired = append(retired, string(cid.cid))
	}
	now := time.Unix(101, 0)
	c.multipath.beginAbandon(1)
	if _, err := c.multipath.receiveAbandon(wire.PathAbandonFrame{PathID: 1}, now, time.Second); err != nil {
		t.Fatal(err)
	}
	c.multipath.collectAbandoned(now.Add(3 * time.Second))
	if err := c.advertisePathCIDs(2); err != nil {
		t.Fatal(err)
	}
	// Replace a path between Sync calls while keeping two path-table entries.
	if len(c.multipath.paths) != 2 {
		t.Fatal("invalid test setup")
	}
	if err := r.Sync(c); err != nil {
		t.Fatal(err)
	}
	for _, cid := range retired {
		if r.ids[cid] != nil {
			t.Fatal("retired path CID stayed routed")
		}
	}
	for _, cid := range c.multipath.paths[2].local.active {
		if r.ids[string(cid.cid)] != c {
			t.Fatal("new path CID missing")
		}
	}
}
