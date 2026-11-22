package transport

import (
	"errors"
	"fmt"
	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
	"testing"
	"time"
)

func TestStreamDataBlockedLifecycle(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, serverWrites := range []bool{false, true} {
			for _, uni := range []bool{false, true} {
				for _, terminal := range []string{"fin", "reset", "partial"} {
					t.Run(fmt.Sprintf("%s/server=%t/uni=%t/%s", vName(v), serverWrites, uni, terminal), func(t *testing.T) {
						p := streamParameters()
						p.InitialMaxStreamDataUni = 1
						p.InitialMaxStreamDataBidiRemote = 1
						p.ResetStreamAt = true
						client, server := testPair(t, v, p)
						drivePair(t, client, server, false, false)
						c, peer := client, server
						if serverWrites {
							c, peer = server, client
						}
						id, err := c.OpenStream(uni)
						if err != nil {
							t.Fatal(err)
						}
						if n, err := c.WriteStream(id, []byte{1}, false); n != 1 || err != nil {
							t.Fatal(n, err)
						}
						drivePair(t, client, server, false, false)
						block := func() {
							t.Helper()
							if n, err := c.WriteStream(id, []byte{2}, false); n != 0 || !errors.Is(err, stream.ErrWouldBlock) {
								t.Fatal(n, err)
							}
						}
						before := c.queuedBytes
						saved := c.queuedBytes
						c.queuedBytes = maxSendBytes
						block()
						c.queuedBytes = saved
						if c.err != nil {
							t.Fatal("advisory queue pressure failed connection", c.err)
						}
						block()
						block()
						space := &c.spaces[applicationSpace]
						if len(space.queue) != 1 || c.queuedBytes != before+32 {
							t.Fatal("missing or duplicate blocked signal")
						}
						original := space.queue[0]
						f, ok := original.frame.(wire.StreamDataBlockedFrame)
						if !ok || f.StreamID != id || f.Limit != 1 {
							t.Fatal(original.frame)
						}
						now := time.Unix(101, 0)
						if packet, err := c.NextDatagram(now); err != nil || len(packet) == 0 {
							t.Fatal("no blocked output", err)
						}
						for i, limit := range []uint64{1, 2, 1} {
							packet := makePeerPacket(t, peer, applicationSpace, uint64(100+i), wire.MaxStreamDataFrame{StreamID: id, Maximum: limit})
							if err := c.HandleDatagram(packet, now); err != nil {
								t.Fatal(err)
							}
							if i == 0 && original.acked {
								t.Fatal("nonincreasing grant retired signal")
							}
						}
						if !original.acked || c.queuedBytes != before {
							t.Fatal("credit failed to retire stale signal")
						}
						if n, err := c.WriteStream(id, []byte{2}, false); n != 1 || err != nil {
							t.Fatal(n, err)
						}
						block()
						var pending *sendItem
						for _, item := range space.queue {
							if f, ok := item.frame.(wire.StreamDataBlockedFrame); ok {
								if f.Limit != 2 {
									t.Fatal(f)
								}
								pending = item
							}
						}
						if pending == nil {
							t.Fatal("missing new limit signal")
						}
						switch terminal {
						case "fin":
							_, err = c.WriteStream(id, nil, true)
						case "reset":
							err = c.CancelWrite(id, 7)
						case "partial":
							err = c.CancelWriteAt(id, 7, 1)
						}
						if err != nil || !pending.acked {
							t.Fatal("terminal send retained blocked signal", err)
						}
						for _, item := range space.queue {
							if _, ok := item.frame.(wire.StreamDataBlockedFrame); ok {
								t.Fatal("stale blocked queue")
							}
						}
					})
				}
			}
		}
	}
}

func TestStreamDataBlockedRequiresStreamExhaustion(t *testing.T) {
	p := streamParameters()
	p.InitialMaxData = 0
	c, peer := testPair(t, wire.Version1, p)
	drivePair(t, c, peer, false, false)
	id, err := c.OpenStream(true)
	if err != nil {
		t.Fatal(err)
	}
	// Connection credit, an empty non-FIN write, and local queue pressure are
	// different constraints. None should report exhaustion of this stream window.
	for _, data := range [][]byte{{1}, nil} {
		if _, err := c.WriteStream(id, data, false); !errors.Is(err, stream.ErrWouldBlock) {
			t.Fatal(err)
		}
	}
	queued := c.queuedBytes
	c.queuedBytes = maxSendBytes
	if _, err := c.WriteStream(id, []byte{1}, false); !errors.Is(err, stream.ErrWouldBlock) {
		t.Fatal(err)
	}
	c.queuedBytes = queued
	if c.streams.objects[id].blockedNotified {
		t.Fatal("spurious stream blocked signal")
	}
	for _, item := range c.spaces[applicationSpace].queue {
		if _, ok := item.frame.(wire.StreamDataBlockedFrame); ok {
			t.Fatal("spurious stream-scoped frame")
		}
	}
}
