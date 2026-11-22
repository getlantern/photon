package transport

import (
	"errors"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestPacketNumberExhaustionOwners(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, mode := range []string{"borrow", "scheduler", "bound", "endpoint"} {
			t.Run(fmt.Sprintf("%x/%s", v, mode), func(t *testing.T) {
				now := time.Unix(200, 0)
				local := netip.MustParseAddrPort("192.0.2.1:1234")
				remote := netip.MustParseAddrPort("192.0.2.2:443")
				if mode == "endpoint" {
					c, peer := testPair(t, v)
					drivePair(t, c, peer, false, false)
					endpoint, err := NewClientEndpoint(c, remote)
					if err != nil {
						t.Fatal(err)
					}
					defer endpoint.Close()
					var terminal error
					endpoint.onTerminal = func(got *Conn, err error) {
						if got != c {
							t.Error("wrong terminal connection")
						}
						terminal = err
					}
					if err := endpoint.sync(c); err != nil {
						t.Fatal(err)
					}
					c.spaces[applicationSpace].nextPN = wire.MaxVarint
					c.enqueue(applicationSpace, wire.PingFrame{}, 1)
					out, err := endpoint.NextDatagram(now)
					if out != nil || !errors.Is(err, wire.ErrPacketNumber) || !errors.Is(terminal, wire.ErrPacketNumber) {
						t.Fatal("endpoint exhaustion", out, err, terminal)
					}
					if len(endpoint.conns) != 0 || len(endpoint.router.entries) != 0 || endpoint.ready.Len() != 0 {
						t.Fatal("terminal connection retained")
					}
					if out, err := endpoint.NextDatagram(now); out != nil || err != nil {
						t.Fatal("removed connection scheduled again", err)
					}
					return
				}
				c, _ := sendingPathPair(t, v)
				source := c.multipath.paths[0].application
				dest := c.multipath.paths[1].application
				dest.validation.state = PathValidated
				dest.budget.Validate()
				dest.space.nextPN = wire.MaxVarint
				if err := c.queueFrameOnPath(0, wire.StreamFrame{StreamID: 0, Data: []byte("borrowed payload")}); err != nil {
					t.Fatal(err)
				}
				// The scheduler starts on idle path 1, which borrows data from path 0.
				c.multipath.haveScheduled = true
				c.multipath.lastScheduled = 0
				var data []byte
				var err error
				switch mode {
				case "borrow":
					data, err = c.nextSharedPathData(1, []uint64{0, 1}, now)
				case "scheduler":
					_, data, err = c.nextMultipathPacket(now)
				case "bound":
					for _, id := range []uint64{0, 1} {
						if err := c.bindPreparedPath(id, local, remote); err != nil {
							t.Fatal(err)
						}
					}
					_, _, data, err = c.nextBoundMultipathPacket(now)
				}
				if !errors.Is(err, wire.ErrPacketNumber) || len(data) != 0 || !c.closed || c.multipath != nil {
					t.Fatal("scheduled exhaustion", err)
				}
				// Retain the former path pointers to check release even after the connection
				// drops its owner. Borrow rollback must not restore the released payload.
				if len(source.space.queue) != 0 || len(dest.space.queue) != 0 || c.queuedBytes != 0 {
					t.Fatal("borrowed payload retained after terminal cleanup")
				}
			})
		}
	}
}
