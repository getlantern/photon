package transport

import (
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestRoutedOutputCollisionDoesNotCommit(t *testing.T) {
	c, other := sendingPathPair(t, wire.Version1)
	local := netip.MustParseAddrPort("192.0.2.1:443")
	peer := netip.MustParseAddrPort("192.0.2.2:444")
	for _, id := range []uint64{0, 1} {
		if err := c.bindPreparedPath(id, local, peer); err != nil {
			t.Fatal(err)
		}
	}
	owner := &networkOwner{conn: c, primary: pathTuple{local, peer}, activationChecked: true}
	first, _ := NewRouter(8, 4)
	second, _ := NewRouter(8, 4)
	for _, r := range []*Router{first, second} {
		if err := r.Add(c, peer); err != nil {
			t.Fatal(err)
		}
	}
	if err := second.Add(other, local); err != nil {
		t.Fatal(err)
	}
	if err := c.advertisePathCIDs(2); err != nil {
		t.Fatal(err)
	}
	// Model a collision with an already reserved identity in the second socket.
	var cid []byte
	for _, id := range c.multipath.paths[2].local.active {
		cid = id.cid
		break
	}
	second.ids[string(cid)] = other
	before := c.spaces[applicationSpace].nextPN
	sealed := c.sealed[applicationSpace]
	if _, _, b, err := owner.nextRouted(time.Unix(101, 0), first, second); !errors.Is(err, ErrRouteCollision) || b != nil {
		t.Fatal(err)
	}
	if c.spaces[applicationSpace].nextPN != before || c.sealed[applicationSpace] != sealed {
		t.Fatal("collision consumed packet state")
	}
	if _, ok := c.multipath.lookupLocal(cid); ok {
		t.Fatal("colliding CID marked sent")
	}
	delete(second.ids, string(cid))
	if _, _, b, err := owner.nextRouted(time.Unix(101, 0), first, second); err != nil || b == nil {
		t.Fatal("reservation retry", err)
	}
	for _, r := range []*Router{first, second} {
		if r.ids[string(cid)] != c {
			t.Fatal("not reserved in every socket")
		}
	}
}
