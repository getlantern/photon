package transport

import (
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestMigrationProbeSendAccounting(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, mode := range []string{"peer-challenge", "local-challenge", "staged-challenge", "response"} {
			t.Run(fmt.Sprintf("%x/%s", uint32(v), mode), func(t *testing.T) {
				c, peer := sendingPathPair(t, v)
				now := time.Unix(101, 0)
				tuple := pathTuple{netip.MustParseAddrPort("192.0.2.1:445"), netip.MustParseAddrPort("192.0.2.2:443")}
				candidate, err := newUncreditedMigrationCandidate(tuple, now, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				candidate.explicitSwitch = mode == "staged-challenge"
				local := mode != "peer-challenge"
				if mode == "response" {
					if err := candidate.queueResponse(wire.PathResponseFrame{Data: [8]byte{1}}); err != nil {
						t.Fatal(err)
					}
				}
				c.publishStatistics()
				before := c.Statistics()
				totals := c.sentTotals
				// A credit-blocked attempt must not contribute an emitted packet.
				if data, _, err := c.sealMigrationProbe(1, candidate, now, local); err != nil || data != nil {
					t.Fatal("credit blocked", err)
				}
				if c.sentTotals != totals || c.Statistics() != before {
					t.Fatal("blocked attempt counted")
				}
				candidate.budget.Validate()
				// The pending-write hold applies even to direct candidate construction.
				c.pendingWrite = &pendingForeignWrite{}
				data, _, err := c.sealMigrationProbe(1, candidate, now, local)
				c.pendingWrite = nil
				if err != nil || data != nil || c.sentTotals != totals {
					t.Fatal("probe bypassed pending write", err)
				}
				for attempt := 0; attempt < 2; attempt++ {
					if attempt != 0 {
						now = now.Add(time.Second)
						if mode == "response" {
							if err := candidate.queueResponse(wire.PathResponseFrame{Data: [8]byte{2}}); err != nil {
								t.Fatal(err)
							}
						}
					}
					data, where, err := c.sealMigrationProbe(1, candidate, now, local)
					if err != nil || len(data) == 0 || where != tuple {
						t.Fatal("missing probe", err)
					}
					decoded, err := peer.decodeMultipathPacket(data, now)
					if err != nil || decoded == nil {
						t.Fatal("invalid emitted packet", err)
					}
					totals.packets++
					totals.bytes += uint64(len(data))
					got := c.Statistics()
					if c.sentTotals != totals || got.PacketsSent != totals.packets || got.BytesSent != totals.bytes {
						t.Fatalf("emitted probe missing from counters: got %+v want packets=%d bytes=%d", got, totals.packets, totals.bytes)
					}
					if got.PacketsReceived != before.PacketsReceived || got.BytesReceived != before.BytesReceived || got.PacketsLost != before.PacketsLost {
						t.Fatal("send changed unrelated counters")
					}
				}
			})
		}
	}
}
