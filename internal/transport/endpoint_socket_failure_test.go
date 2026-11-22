package transport

import (
	"errors"
	"net/netip"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestSocketFailureStopsAllAffectedPathsBeforeEvacuation(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	failed := netip.MustParseAddrPort("192.0.2.1:443")
	healthy := netip.MustParseAddrPort("192.0.2.1:444")
	peer := netip.MustParseAddrPort("192.0.2.2:443")
	if _, err := c.multipath.issue(2); err != nil {
		t.Fatal(err)
	}
	if _, err := c.multipath.receiveNew(pathNew(2, 0, 0, "remote02")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.multipath.application(2); err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint64{0, 1, 2} {
		address := failed
		if id == 2 {
			address = healthy
		}
		if err := c.bindPreparedPath(id, address, peer); err != nil {
			t.Fatal(err)
		}
	}
	e, err := NewClientEndpoint(c, peer)
	if err != nil {
		t.Fatal(err)
	}
	e.networks = map[*Conn]*networkOwner{c: {conn: c, primary: pathTuple{failed, peer}, activationChecked: true}}
	e.locals = map[netip.AddrPort]bool{failed: true, healthy: true}
	var items []*sendItem
	for _, id := range []uint64{0, 1} {
		if err := c.queueFrameOnPath(id, wire.PingFrame{}); err != nil {
			t.Fatal(err)
		}
		q := c.multipath.paths[id].application.space.queue
		items = append(items, q[len(q)-1])
	}
	e.failLocalSocket(failed, errors.New("reader failure"))
	if c.closed || e.locals[failed] || !e.locals[healthy] {
		t.Fatal("surviving runtime removed")
	}
	for _, id := range []uint64{0, 1} {
		if c.multipath.abandoned[id] == nil || len(c.multipath.paths[id].application.space.queue) != 0 {
			t.Fatal("failed socket path retained output")
		}
	}
	q := c.multipath.paths[2].application.space.queue
	for _, want := range items {
		count := 0
		for _, item := range q {
			if item == want {
				count++
			}
		}
		if count != 1 {
			t.Fatal("item did not reach surviving socket exactly once", count)
		}
	}
	e.failLocalSocket(healthy, errors.New("last socket failure"))
	if !c.closed || len(e.conns) != 0 || len(e.router.entries) != 0 {
		t.Fatal("last socket failure retained connection")
	}
}
