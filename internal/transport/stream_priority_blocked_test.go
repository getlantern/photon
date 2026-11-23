package transport

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

func TestStreamPriorityCongestionTransaction(t *testing.T) {
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
				q := &c.spaces[applicationSpace]
				flight := &c.recovery.BytesInFlight
				window := c.congestion.Window()
				next := c.NextDatagram
				if path {
					app := c.multipath.paths[0].application
					q = app.space
					flight = &app.sent.BytesInFlight
					window = app.congestion.Window()
					next = func(now time.Time) ([]byte, error) { return c.nextPacketOnPath(0, now) }
				}
				var ids []uint64
				for i := 0; i < 2; i++ {
					id, err := c.OpenStream(false)
					if err != nil {
						t.Fatal(err)
					}
					ids = append(ids, id)
					if _, err := c.WriteStream(id, []byte("queued"), false); err != nil {
						t.Fatal(err)
					}
				}
				c.SetStreamPriority(ids[1], 0, true)
				before := c.streams.priorityCursor
				pn, charge, oldFlight := q.nextPN, c.queuedBytes, *flight
				*flight = window
				now := time.Unix(102, 0)
				for i := 0; i < 3; i++ {
					if b, err := next(now); err != nil || b != nil {
						t.Fatal("congestion gate", err)
					}
				}
				if c.streams.priorityCursor != before || q.nextPN != pn || c.queuedBytes != charge {
					t.Fatal("blocked packet committed scheduler or transport state")
				}
				// A priority update after blocked selection must replace that selection.
				c.SetStreamPriority(ids[1], 7, true)
				*flight = oldFlight
				b, err := next(now.Add(time.Millisecond))
				if err != nil || b == nil {
					t.Fatal(err)
				}
				item := q.sent[q.largestSent]
				if item == nil || item.frame.(wire.StreamFrame).StreamID != ids[0] {
					t.Fatal("stale priority selection committed")
				}
			})
		}
	}
}

func TestStreamPriorityFlowBlocked(t *testing.T) {
	p := streamParameters()
	p.InitialMaxStreamDataBidiRemote = 4
	c, peer := testPair(t, wire.Version1, p)
	drivePair(t, c, peer, false, false)
	high, err := c.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	low, err := c.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	c.SetStreamPriority(high, 0, false)
	c.SetStreamPriority(low, 7, true)
	if n, err := c.WriteStream(high, []byte("full"), false); err != nil || n != 4 {
		t.Fatal(n, err)
	}
	if n, err := c.WriteStream(high, []byte("blocked"), false); !errors.Is(err, stream.ErrWouldBlock) || n != 0 {
		t.Fatal(n, err)
	}
	if _, err := c.WriteStream(low, []byte("next"), true); err != nil {
		t.Fatal(err)
	}
	q := &c.spaces[applicationSpace]
	sawLow := false
	for i := 0; i < 10; i++ {
		b, err := c.NextDatagram(time.Unix(102, 0).Add(time.Duration(i) * time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			continue
		}
		if item := q.sent[q.largestSent]; item != nil {
			if f, ok := item.frame.(wire.StreamFrame); ok && f.StreamID == low {
				sawLow = true
				break
			}
		}
	}
	if !sawLow {
		t.Fatal("flow-blocked high priority stream prevented lower priority data")
	}
}
