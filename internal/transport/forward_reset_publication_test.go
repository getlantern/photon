package transport

import (
	"net/netip"
	"testing"
)

func TestResetHintSharedIdentityPublication(t *testing.T) {
	for _, mode := range []string{"shared-peers", "shared-tokens", "collapsed-peers", "collapsed-tokens"} {
		t.Run(mode, func(t *testing.T) {
			local := netip.MustParseAddrPort("192.0.2.1:443")
			first := netip.MustParseAddrPort("192.0.2.2:444")
			second := netip.MustParseAddrPort("192.0.2.3:444")
			c := resetPublicationBenchmarkConn(local, first, 2)
			if mode != "shared-peers" {
				c.multipath.paths[1].application.network.remote = second
			}
			ids := c.multipath.paths[1].peer
			entry := ids.active[0]
			if mode == "shared-tokens" {
				entry.token = c.multipath.paths[0].peer.active[0].token
				ids.active[0] = entry
			}
			route := &handoffRoute{local: local}
			route.publishResetHints(c)
			old := route.resetHints.Load()
			switch mode {
			case "collapsed-peers":
				c.multipath.paths[1].application.network.remote = first
			case "collapsed-tokens":
				entry.token = c.multipath.paths[0].peer.active[0].token
				ids.active[0] = entry
			}
			route.publishResetHints(c)
			current := route.resetHints.Load()
			switch mode {
			case "shared-peers", "shared-tokens":
				if current != old {
					t.Fatal("unchanged shared identity republished")
				}
			case "collapsed-peers":
				if current == old || len(current.peers) != 1 {
					t.Fatal("removed peer retained after identity collapse")
				}
				if _, ok := current.peers[second]; ok {
					t.Fatal("stale peer hint")
				}
				if len(old.peers) != 2 {
					t.Fatal("published snapshot mutated")
				}
			case "collapsed-tokens":
				if current == old || len(current.tokens) != 1 {
					t.Fatal("removed token retained after identity collapse")
				}
				if len(old.tokens) != 2 {
					t.Fatal("published token snapshot mutated")
				}
			}
			if allocations := testing.AllocsPerRun(100, func() { route.publishResetHints(c) }); allocations != 0 {
				t.Fatalf("unchanged publication allocates: %g", allocations)
			}
			if route.resetHints.Load() != current {
				t.Fatal("stable snapshot was replaced")
			}
		})
	}
}

func TestResetHintPublicationRetirement(t *testing.T) {
	local := netip.MustParseAddrPort("192.0.2.1:443")
	c := resetPublicationBenchmarkConn(local, netip.MustParseAddrPort("192.0.2.2:444"), 2)
	route := &handoffRoute{local: local}
	route.publishResetHints(c)
	old := route.resetHints.Load()
	ids := c.multipath.paths[1].peer
	entry := ids.active[0]
	delete(ids.active, 0)
	route.publishResetHints(c)
	if next := route.resetHints.Load(); next == old || len(next.tokens) != 1 {
		t.Fatal("retired CID token retained")
	}
	entry.used = false
	ids.active[1] = entry
	route.publishResetHints(c)
	if len(route.resetHints.Load().tokens) != 1 {
		t.Fatal("unused replacement token published")
	}
	entry.used = true
	ids.active[1] = entry
	route.publishResetHints(c)
	if len(route.resetHints.Load().tokens) != 2 {
		t.Fatal("first-use token not published")
	}
	c.multipath.abandoned = map[uint64]*pathAbandonment{1: {}}
	route.publishResetHints(c)
	if len(route.resetHints.Load().tokens) != 1 {
		t.Fatal("abandoned path token retained")
	}
	c.multipath = nil
	route.publishResetHints(c)
	if len(route.resetHints.Load().tokens) != 0 || len(route.resetHints.Load().peers) != 0 {
		t.Fatal("removed path state retained")
	}
	empty := route.resetHints.Load()
	route.publishResetHints(c)
	if route.resetHints.Load() != empty {
		t.Fatal("empty publication churn")
	}
}
