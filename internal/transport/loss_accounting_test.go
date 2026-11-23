package transport

import (
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func TestConnectionLossAccounting(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, multipath := range []bool{false, true} {
			for _, timeout := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/multipath=%t/timeout=%t", version, multipath, timeout), func(t *testing.T) {
					var c *Conn
					if multipath {
						c, _ = sendingPathPair(t, version)
					} else {
						var peer *Conn
						c, peer = testPair(t, version)
						drivePair(t, c, peer, false, false)
					}
					c.loss = lossAccounting{}
					ledger, _ := recovery.NewSent(16)
					if multipath {
						c.multipath.paths[1].application.sent = ledger
						c.multipath.paths[1].application.budget.Validate()
					} else {
						c.recovery = ledger
					}
					start := time.Unix(101, 0)
					for pn := uint64(0); pn < 4; pn++ {
						if err := ledger.OnSent(recovery.Application, recovery.Packet{Number: pn, SentAt: start, Bytes: int(100 + pn), AckEliciting: true, InFlight: true}); err != nil {
							t.Fatal(err)
						}
					}
					now := start.Add(time.Millisecond)
					ack := func(pn uint64) {
						t.Helper()
						f := wire.AckFrame{Ranges: []wire.AckRange{{Smallest: pn, Largest: pn}}}
						var err error
						if multipath {
							err = c.dispatchMultipathPacket(&pathPacket{pathID: 1, receipt: NewPacket, frames: []wire.Frame{wire.PathAckFrame{PathID: 1, AckFrame: f}}}, now)
						} else {
							err = c.handleAck(applicationSpace, f, now)
						}
						if err != nil {
							t.Fatal(err)
						}
					}
					if timeout {
						ack(1)
						if c.loss != (lossAccounting{}) {
							t.Fatal("early loss", c.loss)
						}
						deadline, _, ok := ledger.LossTime()
						if !ok {
							t.Fatal("no loss deadline")
						}
						if !multipath {
							c.armTimer(now)
						}
						now = deadline
						if err := c.HandleTimeout(now); err != nil {
							t.Fatal(err)
						}
					} else {
						ack(3)
					}
					want := lossAccounting{declaredPackets: 1, declaredBytes: 100}
					if c.loss != want {
						t.Fatal("loss declaration", c.loss, want)
					}
					ack(0)
					want.recoveredPackets, want.recoveredBytes = 1, 100
					if c.loss != want {
						t.Fatal("late correction", c.loss, want)
					}
					ack(0)
					if multipath {
						if err := c.abandonFailedPath(1); err != nil {
							t.Fatal(err)
						}
					} else {
						ledger.Discard(recovery.Application)
					}
					if c.loss != want {
						t.Fatal("duplicate ACK or discard changed totals", c.loss, want)
					}
				})
			}
		}
	}
}
