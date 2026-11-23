package transport

import (
	"errors"
	"fmt"
	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
	"testing"
	"time"
)

func TestDataBlockedSharedLifecycle(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, serverWrites := range []bool{false, true} {
			for _, uni := range []bool{false, true} {
				for _, terminal := range []string{"fin", "reset", "partial"} {
					t.Run(fmt.Sprintf("%s/server=%t/uni=%t/%s", vName(v), serverWrites, uni, terminal), func(t *testing.T) {
						p := streamParameters()
						p.InitialMaxData = 1
						p.ResetStreamAt = true
						client, server := testPair(t, v, p)
						drivePair(t, client, server, false, false)
						c, peer := client, server
						if serverWrites {
							c, peer = server, client
						}
						first, err := c.OpenStream(uni)
						if err != nil {
							t.Fatal(err)
						}
						second, err := c.OpenStream(uni)
						if err != nil {
							t.Fatal(err)
						}
						if n, err := c.WriteStream(first, []byte{1}, false); n != 1 || err != nil {
							t.Fatal(n, err)
						}
						drivePair(t, client, server, false, false)
						block := func(id uint64) {
							t.Helper()
							if n, err := c.WriteStream(id, []byte{2}, false); n != 0 || !errors.Is(err, stream.ErrWouldBlock) {
								t.Fatal(n, err)
							}
						}
						saved := c.queuedBytes
						c.queuedBytes = maxSendBytes
						block(first)
						c.queuedBytes = saved
						if c.err != nil || c.streams.dataBlockedNotified {
							t.Fatal("queue pressure is not a notification", c.err)
						}
						block(first)
						block(second)
						block(first)
						if c.streams.dataBlockedCount != 2 {
							t.Fatal("duplicate or missing waiter", c.streams.dataBlockedCount)
						}
						find := func(limit uint64) *sendItem {
							t.Helper()
							var found *sendItem
							for _, item := range c.spaces[applicationSpace].queue {
								if f, ok := item.frame.(wire.DataBlockedFrame); ok {
									if found != nil || f.Limit != limit {
										t.Fatal("duplicate or stale signal", f)
									}
									found = item
								}
							}
							if found == nil {
								t.Fatal("missing DATA_BLOCKED")
							}
							return found
						}
						original := find(1)
						now := time.Unix(101, 0)
						if packet, err := c.NextDatagram(now); err != nil || len(packet) == 0 {
							t.Fatal("signal output", err)
						}
						var lost []recovery.Packet
						for pn, item := range c.spaces[applicationSpace].sent {
							if item == original {
								lost = append(lost, recovery.Packet{Number: pn})
							}
						}
						if len(lost) == 0 {
							t.Fatal("missing transmitted notification")
						}
						charged := c.queuedBytes
						c.queueLost(applicationSpace, lost)
						if !original.queued || c.queuedBytes != charged {
							t.Fatal("notification loss changed charge")
						}
						packet := makePeerPacket(t, peer, applicationSpace, 100, wire.MaxDataFrame{Maximum: 1})
						if err := c.HandleDatagram(packet, now); err != nil {
							t.Fatal(err)
						}
						if original.acked || c.streams.dataBlockedCount != 2 {
							t.Fatal("equal grant changed blocked state")
						}
						end := func(id uint64) {
							t.Helper()
							var err error
							switch terminal {
							case "fin":
								_, err = c.WriteStream(id, nil, true)
							case "reset":
								err = c.CancelWrite(id, 7)
							case "partial":
								err = c.CancelWriteAt(id, 7, c.streams.objects[id].send.Offset())
							}
							if err != nil {
								t.Fatal(err)
							}
						}
						end(first)
						if original.acked || c.streams.dataBlockedCount != 1 {
							t.Fatal("one terminal stream removed shared signal")
						}
						end(second)
						if !original.acked || c.streams.dataBlockedCount != 0 || c.streams.dataBlockedNotified {
							t.Fatal("last waiter retained shared signal")
						}
						third, err := c.OpenStream(uni)
						if err != nil {
							t.Fatal(err)
						}
						block(third)
						next := find(1)
						if next == original {
							t.Fatal("reused retired signal")
						}
						for i, limit := range []uint64{2, 1} {
							packet := makePeerPacket(t, peer, applicationSpace, uint64(101+i), wire.MaxDataFrame{Maximum: limit})
							if err := c.HandleDatagram(packet, now); err != nil {
								t.Fatal(err)
							}
						}
						if !next.acked || c.streams.dataBlockedCount != 0 || c.streams.objects[third].connectionBlocked {
							t.Fatal("grant retained waiting state")
						}
						if n, err := c.WriteStream(third, []byte{2}, false); n != 1 || err != nil {
							t.Fatal("fresh connection credit", n, err)
						}
						block(third)
						find(2)
						if c.streams.send.Used() != 2 {
							t.Fatal("signal changed byte accounting")
						}
					})
				}
			}
		}
	}
}

func TestDataBlockedAndStreamBlockedIndependentGrants(t *testing.T) {
	p := streamParameters()
	p.InitialMaxData = 0
	p.InitialMaxStreamDataUni = 0
	c, peer := testPair(t, wire.Version1, p)
	drivePair(t, c, peer, false, false)
	id, err := c.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.WriteStream(id, []byte{1}, false); !errors.Is(err, stream.ErrWouldBlock) {
		t.Fatal(err)
	}
	st := c.streams.objects[id]
	if !st.blockedNotified || !st.connectionBlocked || c.queuedBytes != 64 {
		t.Fatal("simultaneous exhaustion missing signals")
	}
	if err := c.handleStreamFrame(wire.MaxStreamDataFrame{StreamID: id, Maximum: 1}); err != nil {
		t.Fatal(err)
	}
	if st.blockedNotified || !st.connectionBlocked || c.queuedBytes != 32 {
		t.Fatal("stream grant changed connection signal")
	}
	if _, err := c.WriteStream(id, []byte{1}, false); !errors.Is(err, stream.ErrWouldBlock) {
		t.Fatal(err)
	}
	if c.streams.dataBlockedCount != 1 || c.queuedBytes != 32 {
		t.Fatal("connection signal duplicated")
	}
	if err := c.handleStreamFrame(wire.MaxDataFrame{Maximum: 1}); err != nil {
		t.Fatal(err)
	}
	if st.connectionBlocked || c.queuedBytes != 0 {
		t.Fatal("connection grant retained signal")
	}
	if n, err := c.WriteStream(id, []byte{1}, false); n != 1 || err != nil {
		t.Fatal(n, err)
	}
}
