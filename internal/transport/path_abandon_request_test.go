package transport

import (
	"errors"
	"net/netip"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestRequestedAbandonPreflight(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	local := netip.MustParseAddrPort("192.0.2.1:443")
	remote := netip.MustParseAddrPort("192.0.2.2:444")
	for _, id := range []uint64{0, 1} {
		if err := c.bindPreparedPath(id, local, remote); err != nil {
			t.Fatal(err)
		}
	}
	source := c.multipath.paths[0].application
	dest := c.multipath.paths[1].application
	unchanged := func(bytes, queued int) {
		t.Helper()
		if c.Err() != nil || c.multipath.abandoned[0] != nil || c.queuedBytes != bytes || len(dest.space.queue) != queued || len(c.multipath.paths[0].peer.active) == 0 {
			t.Fatal("rejected request mutated live connection")
		}
	}
	before, queued := c.queuedBytes, len(dest.space.queue)
	if err := c.abandonRequestedPath(0); err == nil {
		t.Fatal("accepted unvalidated alternative")
	}
	unchanged(before, queued)
	if err := c.abandonRequestedPath(3); err == nil {
		t.Fatal("accepted uninitiated path")
	}
	unchanged(before, queued)
	dest.validation.state = PathValidated
	originalBytes := c.queuedBytes
	c.queuedBytes = maxSendBytes
	if err := c.abandonRequestedPath(0); !errors.Is(err, ErrSendBuffer) {
		t.Fatal(err)
	}
	unchanged(maxSendBytes, queued)
	c.queuedBytes = originalBytes
	if err := c.queueFrameOnPath(0, wire.PingFrame{}); err != nil {
		t.Fatal(err)
	}
	item := source.space.queue[len(source.space.queue)-1]
	before = c.queuedBytes
	if err := c.abandonRequestedPath(0); err != nil {
		t.Fatal(err)
	}
	if c.multipath.abandoned[0] == nil || len(source.space.queue) != 0 || !item.queued || item.acked {
		t.Fatal("failed evacuation")
	}
	count := 0
	for _, entry := range dest.space.queue {
		if entry == item {
			count++
		}
	}
	if count != 1 || c.queuedBytes != before+32 {
		t.Fatal("evacuation duplicated data or charged it twice")
	}
	before, queued = c.queuedBytes, len(dest.space.queue)
	if err := c.abandonRequestedPath(0); err != nil {
		t.Fatal(err)
	}
	if c.queuedBytes != before || len(dest.space.queue) != queued {
		t.Fatal("duplicate abandonment queued control")
	}
	if err := c.abandonRequestedPath(1); err == nil {
		t.Fatal("accepted final path abandonment")
	}
	if c.Err() != nil || c.multipath.abandoned[1] != nil {
		t.Fatal("rejection killed final path")
	}
}
