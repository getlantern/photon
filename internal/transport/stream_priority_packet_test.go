package transport

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func TestStreamPriorityPacketOrder(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, path := range []bool{false, true} {
			t.Run(fmt.Sprintf("%v/path=%v", version, path), func(t *testing.T) {
				var c, peer *Conn
				if path {
					c, peer = sendingPathPair(t, version)
				} else {
					c, peer = testPair(t, version, streamParameters())
					drivePair(t, c, peer, false, false)
				}
				queue := &c.spaces[applicationSpace]
				if path {
					queue = c.multipath.paths[0].application.space
				}
				var ids []uint64
				for i := 0; i < 3; i++ {
					id, err := c.OpenStream(false)
					if err != nil {
						t.Fatal(err)
					}
					ids = append(ids, id)
					for j := 0; j < 2; j++ {
						if _, err := c.WriteStream(id, []byte{byte(i), byte(j)}, j == 1); err != nil {
							t.Fatal(err)
						}
					}
				}
				c.SetStreamPriority(ids[2], 0, true)
				var got []uint64
				for step := 0; step < 50 && len(got) < 6; step++ {
					now := time.Unix(102, 0).Add(time.Duration(step) * time.Millisecond)
					var packet []byte
					var err error
					if path {
						packet, err = c.nextPacketOnPath(0, now)
					} else {
						packet, err = c.NextDatagram(now)
					}
					if err != nil {
						t.Fatal(err)
					}
					if packet == nil {
						continue
					}
					if item := queue.sent[queue.largestSent]; item != nil {
						if frame, ok := item.frame.(wire.StreamFrame); ok {
							got = append(got, frame.StreamID)
						}
					}
					if path {
						err = peer.handleMultipathPacket(packet, now)
					} else {
						err = peer.HandleDatagram(packet, now)
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				want := []uint64{ids[2], ids[2], ids[0], ids[1], ids[0], ids[1]}
				if !reflect.DeepEqual(got, want) {
					t.Fatal(got, want)
				}
			})
		}
	}
}

func TestStreamPriorityBorrowedPath(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	target := c.multipath.paths[1].application
	var ids []uint64
	for i := 0; i < 2; i++ {
		id, err := c.OpenStream(false)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
		if _, err = c.WriteStream(id, []byte("borrowed"), false); err != nil {
			t.Fatal(err)
		}
	}
	target.budget.Validate()
	target.validation.state = PathValidated
	c.SetStreamPriority(ids[1], 0, true)
	b, err := c.nextSharedPathData(1, []uint64{0, 1}, time.Unix(102, 0))
	if err != nil || b == nil {
		t.Fatal(err)
	}
	item := target.space.sent[target.space.largestSent]
	if item == nil || item.frame.(wire.StreamFrame).StreamID != ids[1] {
		t.Fatal("borrow ignored priority", item)
	}
}

func TestStreamPriorityLostPacket(t *testing.T) {
	c, peer := testPair(t, wire.Version1, streamParameters())
	drivePair(t, c, peer, false, false)
	low, err := c.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	high, err := c.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	c.SetStreamPriority(low, 7, true)
	c.SetStreamPriority(high, 0, true)
	if _, err := c.WriteStream(low, []byte("lost"), false); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(102, 0)
	packet, err := c.NextDatagram(now)
	if err != nil || packet == nil {
		t.Fatal(err)
	}
	q := &c.spaces[applicationSpace]
	old := q.sent[q.largestSent]
	if old == nil || !old.transmitted {
		t.Fatal("packet not tracked as transmitted")
	}
	if _, err := c.WriteStream(high, []byte("new"), false); err != nil {
		t.Fatal(err)
	}
	queueLostItems(q, q, []recovery.Packet{{Number: q.largestSent}})
	packet, err = c.NextDatagram(now.Add(time.Millisecond))
	if err != nil || packet == nil {
		t.Fatal(err)
	}
	if q.sent[q.largestSent] != old {
		t.Fatal("new data displaced retransmission")
	}
}
