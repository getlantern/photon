package transport

import (
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestMultipathShutdownReleasesAllPaths(t *testing.T) {
	for _, drain := range []bool{false, true} {
		c, _ := sendingPathPair(t, wire.Version1)
		now := time.Unix(101, 0)
		app := c.multipath.paths[1].application
		local := netip.MustParseAddrPort("192.0.2.1:443")
		remote := netip.MustParseAddrPort("192.0.2.2:444")
		if err := c.bindPreparedPath(1, local, remote); err != nil {
			t.Fatal(err)
		}
		app.budget.Validate()
		if err := c.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
			t.Fatal(err)
		}
		if b, err := c.nextPacketOnPath(1, now); err != nil || b == nil {
			t.Fatal(err)
		}
		if err := c.queueFrameOnPath(1, wire.StreamFrame{StreamID: 0, Data: []byte("release me")}); err != nil {
			t.Fatal(err)
		}
		if _, err := app.space.received.Record(2, now); err != nil {
			t.Fatal(err)
		}
		app.probePending = true
		app.pacingAt = now.Add(time.Second)
		app.validation.state = PathValidating
		app.keyNumbers.haveLargest = true
		var cid []byte
		for _, id := range c.multipath.paths[1].local.active {
			cid = id.cid
			break
		}
		if drain {
			c.beginDraining(ErrPeerClosed, now)
		} else if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		if app.space.queue != nil || app.space.sent != nil || app.space.received != nil || !app.space.discarded || app.sent.BytesInFlight != 0 || !app.timer.Deadline().IsZero() || app.probePending || !app.pacingAt.IsZero() || app.keyNumbers.haveLargest || app.validation.state != PathUnchecked {
			t.Fatal("path resources survived shutdown")
		}
		if c.queuedBytes != 0 {
			t.Fatal("queue charge survived shutdown")
		}
		if drain {
			if c.multipath == nil || app.network == nil {
				t.Fatal("draining lost routing")
			}
			if id, ok := c.multipath.lookupLocal(cid); !ok || id != 1 {
				t.Fatal("draining lost CID route")
			}
			deadline := c.RecoveryDeadline()
			if err := c.HandleTimeout(deadline.Add(-time.Nanosecond)); err != nil {
				t.Fatal(err)
			}
			if c.multipath == nil {
				t.Fatal("early routing disposal")
			}
			if err := c.HandleTimeout(deadline); err != nil {
				t.Fatal(err)
			}
		}
		if c.multipath != nil || !c.RecoveryDeadline().IsZero() {
			t.Fatal("closed connection retained routing")
		}
	}
}
