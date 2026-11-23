package transport

import (
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestForwardPublicationTracksLatestSnapshot(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		c, _ := testPair(t, version)
		owner, _ := NewRouter(8, 8)
		if err := owner.Add(c, netip.MustParseAddrPort("192.0.2.1:443")); err != nil {
			t.Fatal(err)
		}
		h := newPacketHandoff()
		defer h.close()
		owner.handoff = h
		a, _ := NewRouter(8, 8)
		b, _ := NewRouter(8, 8)
		ar := updateTestRoute(t, a, h, nil)
		br := updateTestRoute(t, b, h, nil)
		for _, route := range []*handoffRoute{ar, br} {
			if err := h.bindConnection(route, c); err != nil {
				t.Fatal(err)
			}
		}
		notify := make(chan struct{}, 1)
		check := func(want bool) {
			t.Helper()
			ready, err := owner.publishForwardCIDs(c, notify)
			if err != nil || ready != want {
				t.Fatalf("ready=%t, want %t: %v", ready, want, err)
			}
		}
		check(false)
		// Issuance while the first snapshot is pending must not be covered by its ACK.
		frames, err := c.localIDs.issue(4)
		if err != nil || len(frames) != 3 {
			t.Fatal(err)
		}
		a.applyForwardUpdates()
		b.applyForwardUpdates()
		check(false)
		if len(a.foreignIDs) != 1 || len(b.foreignIDs) != 1 {
			t.Fatal("old acknowledgement advertised new IDs")
		}
		a.applyForwardUpdates()
		check(false) // Every foreign source must acknowledge the current set.
		b.applyForwardUpdates()
		check(true)
		revision := owner.entries[c].revision
		for range 3 {
			check(true)
		}
		if owner.entries[c].revision != revision || a.forwardPendingIDs.Load() != 0 || b.forwardPendingIDs.Load() != 0 {
			t.Fatal("unchanged snapshot reissued")
		}
		if err := h.bindConnection(ar, c); err != nil {
			t.Fatal(err)
		}
		check(true) // Rebinding the same route must preserve publication state.
		retired := frames[0]
		c.localIDs.markSent(retired.Sequence)
		if _, err := c.localIDs.retire(retired.Sequence, c.config.LocalCID); err != nil {
			t.Fatal(err)
		}
		replacement, err := c.localIDs.issue(4)
		if err != nil || len(replacement) != 1 {
			t.Fatal(err)
		}
		check(false)
		a.applyForwardUpdates()
		b.applyForwardUpdates()
		check(true)
		for _, source := range []*Router{a, b} {
			if source.foreignIDs[string(retired.ConnectionID)] != nil || source.foreignIDs[string(replacement[0].ConnectionID)] == nil {
				t.Fatal("same-count retirement/replacement missed")
			}
		}
		owner.Remove(c)
		if ready, err := owner.publishForwardCIDs(c, notify); ready || err == nil {
			t.Fatal("removed connection ready", err)
		}
	}
}

func TestForwardPublicationCollision(t *testing.T) {
	c, _ := testPair(t, wire.Version1)
	owner, _ := NewRouter(8, 8)
	if err := owner.Add(c, netip.MustParseAddrPort("192.0.2.1:443")); err != nil {
		t.Fatal(err)
	}
	h := newPacketHandoff()
	defer h.close()
	owner.handoff = h
	source, _ := NewRouter(8, 8)
	route := updateTestRoute(t, source, h, nil)
	blocker := updateTestRoute(t, source, h, [][]byte{c.config.LocalCID})
	if err := h.bindConnection(route, c); err != nil {
		t.Fatal(err)
	}
	if ready, err := owner.publishForwardCIDs(c, nil); ready || err != nil {
		t.Fatal(ready, err)
	}
	source.applyForwardUpdates()
	for range 2 {
		if ready, err := owner.publishForwardCIDs(c, nil); ready || !errors.Is(err, ErrRouteCollision) {
			t.Fatal("failed publication ready", ready, err)
		}
	}
	if source.foreignIDs[string(c.config.LocalCID)] != blocker || source.forwardPendingIDs.Load() != 0 {
		t.Fatal("collision changed ownership or queued retries")
	}
	if c.closed || len(owner.entries) != 1 {
		t.Fatal("publication failure removed primary connection")
	}
}

func TestForwardPublicationMultipathSnapshot(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		c, _ := sendingPathPair(t, version)
		owner, _ := NewRouter(8, 8)
		if err := owner.Add(c, netip.MustParseAddrPort("192.0.2.1:443")); err != nil {
			t.Fatal(err)
		}
		h := newPacketHandoff()
		defer h.close()
		owner.handoff = h
		source, _ := NewRouter(8, 8)
		route := updateTestRoute(t, source, h, nil)
		if err := h.bindConnection(route, c); err != nil {
			t.Fatal(err)
		}
		if ready, err := owner.publishForwardCIDs(c, nil); ready || err != nil {
			t.Fatal(ready, err)
		}
		source.applyForwardUpdates()
		if ready, err := owner.publishForwardCIDs(c, nil); !ready || err != nil {
			t.Fatal(ready, err)
		}
		if err := c.advertisePathCIDs(2); err != nil {
			t.Fatal(err)
		}
		if ready, err := owner.publishForwardCIDs(c, nil); ready || err != nil {
			t.Fatal("new path prematurely ready", ready, err)
		}
		source.applyForwardUpdates()
		if ready, err := owner.publishForwardCIDs(c, nil); !ready || err != nil {
			t.Fatal(ready, err)
		}
		for _, path := range c.multipath.paths {
			for _, id := range path.local.active {
				if source.foreignIDs[string(id.cid)] != route {
					t.Fatal("multipath CID not reserved")
				}
			}
		}
	}
}

func TestForwardPublicationAcrossRuntimeOwners(t *testing.T) {
	source, destination := forwardingRuntime(t), forwardingRuntime(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	h, err := destination.handoffTarget(ctx)
	if err != nil {
		t.Fatal(err)
	}
	route, err := source.registerHandoffRoute(ctx, h, nil)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := testPair(t, wire.Version1)
	cid := string(c.config.LocalCID)
	if err := destination.Do(ctx, func(e *ServerEndpoint) error { return e.router.Add(c, netip.MustParseAddrPort("192.0.2.1:443")) }); err != nil {
		t.Fatal(err)
	}
	if err := destination.bindHandoffConnection(ctx, route, c); err != nil {
		t.Fatal(err)
	}
	notify := make(chan struct{}, 1)
	for {
		ready := false
		if err := destination.Do(ctx, func(e *ServerEndpoint) error {
			var err error
			ready, err = e.router.publishForwardCIDs(c, notify)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if ready {
			break
		}
		select {
		case <-notify:
		case <-ctx.Done():
			t.Fatal("publication acknowledgement did not wake requester")
		}
	}
	if err := source.Do(ctx, func(e *ServerEndpoint) error {
		if e.router.foreignIDs[cid] != route {
			return errors.New("acknowledged CID not installed")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := destination.Do(ctx, func(e *ServerEndpoint) error { e.router.Remove(c); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := source.Wait(ctx, func(e *ServerEndpoint) (bool, error) {
		return len(e.router.foreignIDs) == 0 && e.router.forwardPendingIDs.Load() == 0, nil
	}); err != nil {
		t.Fatal(err)
	}
}
