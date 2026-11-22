package transport

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

func TestStreamsBlockedLimitLifecycle(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, serverOpens := range []bool{false, true} {
			for _, uni := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/server=%t/uni=%t", vName(v), serverOpens, uni), func(t *testing.T) {
					p := streamParameters()
					p.InitialMaxStreamsBidi = 0
					p.InitialMaxStreamsUni = 0
					client, server := testPair(t, v, p)
					drivePair(t, client, server, false, false)
					c, peer := client, server
					if serverOpens {
						c, peer = server, client
					}
					block := func() {
						t.Helper()
						if _, err := c.OpenStream(uni); !errors.Is(err, stream.ErrWouldBlock) {
							t.Fatal(err)
						}
					}
					// Advisory signaling must remain retryable under local queue pressure.
					c.queuedBytes = maxSendBytes
					block()
					if c.err != nil {
						t.Fatal(c.err)
					}
					c.queuedBytes = 0
					block()
					s := &c.spaces[applicationSpace]
					if len(s.queue) != 1 || c.queuedBytes != 32 || len(c.streams.objects) != 0 {
						t.Fatal("missing bounded notification")
					}
					item := s.queue[0]
					if f, ok := item.frame.(wire.StreamsBlockedFrame); !ok || f.Limit != 0 || f.Unidirectional != uni {
						t.Fatal(item.frame)
					}
					block()
					if len(s.queue) != 1 || c.queuedBytes != 32 {
						t.Fatal("duplicate notification")
					}
					now := time.Unix(101, 0)
					if packet, err := c.NextDatagram(now); err != nil || len(packet) == 0 {
						t.Fatal("blocked output", err)
					}
					// Simulate recovery declaring the notification's packet lost. Existing
					// reliable ownership must requeue it without charging it twice.
					var lost []recovery.Packet
					for pn, sent := range s.sent {
						if sent == item {
							lost = append(lost, recovery.Packet{Number: pn})
						}
					}
					if len(lost) == 0 {
						t.Fatal("no transmitted notification mapping")
					}
					c.queueLost(applicationSpace, lost)
					if len(s.queue) != 1 || s.queue[0] != item || c.queuedBytes != 32 {
						t.Fatal("lost notification not requeued")
					}
					for i, limit := range []uint64{0, 1, 0} {
						packet := makePeerPacket(t, peer, applicationSpace, uint64(100+i), wire.MaxStreamsFrame{Unidirectional: uni, Maximum: limit})
						if err := c.HandleDatagram(packet, now); err != nil {
							t.Fatal(err)
						}
						if i == 0 && item.acked {
							t.Fatal("unchanged limit retired blocked signal")
						}
					}
					if !item.acked || c.queuedBytes != 0 {
						t.Fatal("new limit retained stale blocked frame")
					}
					if _, err := c.OpenStream(uni); err != nil {
						t.Fatal("new credit did not unblock", err)
					}
					block()
					if len(s.queue) != 1 {
						t.Fatal("missing new-limit notification")
					}
					if f, ok := s.queue[0].frame.(wire.StreamsBlockedFrame); !ok || f.Limit != 1 {
						t.Fatal("stale reported limit", s.queue[0].frame)
					}
				})
			}
		}
	}
}
