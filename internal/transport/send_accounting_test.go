package transport

import (
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestCommittedSendAccounting(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, multipath := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/multipath=%t", version, multipath), func(t *testing.T) {
				var c *Conn
				if multipath {
					c, _ = sendingPathPair(t, version)
				} else {
					var peer *Conn
					c, peer = testPair(t, version)
					drivePair(t, c, peer, false, false)
				}
				if c.sentTotals.packets == 0 || c.sentTotals.bytes < 1200 {
					t.Fatal("handshake sends missing", c.sentTotals)
				}
				want := c.sentTotals
				now := time.Unix(101, 0)
				var packet []byte
				var err error
				if multipath {
					c.multipath.paths[1].application.budget.Validate()
					err = c.queueFrameOnPath(1, wire.PingFrame{})
					if err == nil {
						packet, err = c.nextPacketOnPath(1, now)
					}
				} else {
					err = c.enqueue(applicationSpace, wire.PingFrame{}, 1)
					if err == nil {
						packet, err = c.NextDatagram(now)
					}
				}
				if err != nil || len(packet) == 0 {
					t.Fatal("missing send", err)
				}
				want.packets++
				want.bytes += uint64(len(packet))
				if c.sentTotals != want {
					t.Fatal("packet total", c.sentTotals, want)
				}
				if err := c.CloseWithError(7, "done", now); err != nil {
					t.Fatal(err)
				}
				if c.sentTotals != want {
					t.Fatal("sealing close counted as emission")
				}
				packet, err = c.NextDatagram(now)
				if err != nil || len(packet) == 0 {
					t.Fatal("missing close", err)
				}
				want.packets++
				want.bytes += uint64(len(packet))
				if c.sentTotals != want {
					t.Fatal("close total", c.sentTotals, want)
				}
				if packet, err := c.NextDatagram(now); err != nil || len(packet) != 0 || c.sentTotals != want {
					t.Fatal("suppressed output counted", err)
				}
			})
		}
	}
}
