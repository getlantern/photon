package transport

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

func TestByteBlockedMultipathRetirement(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, uni := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/uni=%t", vName(v), uni), func(t *testing.T) {
				p := streamParameters()
				p.InitialMaxData = 0
				p.InitialMaxStreamDataBidiRemote = 0
				p.InitialMaxStreamDataUni = 0
				c, peer := sendingPathPair(t, v, p)
				for _, conn := range []*Conn{c, peer} {
					app := conn.multipath.paths[1].application
					app.validation.state = PathValidated
					app.budget.Validate()
				}
				var ids [2]uint64
				for i := range ids {
					id, err := c.OpenStream(uni)
					if err != nil {
						t.Fatal(err)
					}
					ids[i] = id
					if _, err := c.WriteStream(id, []byte{1}, false); !errors.Is(err, stream.ErrWouldBlock) {
						t.Fatal(err)
					}
				}
				primary := c.multipath.paths[0].application.space
				if len(primary.queue) != 3 || c.streams.dataBlockedCount != 2 {
					t.Fatal("missing blocked signals")
				}
				items := append([]*sendItem(nil), primary.queue...)
				now := time.Unix(101, 0)
				for len(primary.queue) > 0 {
					if packet, err := c.nextPacketOnPath(0, now); err != nil || packet == nil {
						t.Fatal("signal output", err)
					}
				}
				for _, item := range items {
					sent := false
					for _, mapped := range primary.sent {
						sent = sent || mapped == item
					}
					if !sent {
						t.Fatal("missing sent notification")
					}
				}
				if err := c.abandonFailedPath(0); err != nil {
					t.Fatal(err)
				}
				survivor := c.multipath.paths[1].application.space
				for _, item := range items {
					queued := false
					for _, mapped := range survivor.queue {
						queued = queued || mapped == item
					}
					if !queued || item.acked {
						t.Fatal("notification did not evacuate")
					}
				}
				before := c.queuedBytes
				if err := peer.queueFrameOnPath(1, wire.MaxDataFrame{Maximum: 2}); err != nil {
					t.Fatal(err)
				}
				for _, id := range ids {
					if err := peer.queueFrameOnPath(1, wire.MaxStreamDataFrame{StreamID: id, Maximum: 1}); err != nil {
						t.Fatal(err)
					}
				}
				for len(peer.multipath.paths[1].application.space.queue) > 0 {
					packet, err := peer.nextPacketOnPath(1, now)
					if err != nil || packet == nil {
						t.Fatal("grant output", err)
					}
					if err := c.handleMultipathPacket(packet, now); err != nil {
						t.Fatal(err)
					}
				}
				if c.queuedBytes != before-96 || c.streams.dataBlockedCount != 0 || c.streams.dataBlockedNotified {
					t.Fatal("grants did not retire shared state exactly once")
				}
				for _, item := range items {
					if !item.acked {
						t.Fatal("notification still live")
					}
					for _, state := range c.multipath.paths {
						for _, mapped := range state.application.space.queue {
							if mapped == item {
								t.Fatal("stale queue mapping")
							}
						}
						for _, mapped := range state.application.space.sent {
							if mapped == item {
								t.Fatal("stale sent mapping")
							}
						}
					}
				}
				for _, id := range ids {
					if n, err := c.WriteStream(id, []byte{1}, false); n != 1 || err != nil {
						t.Fatal("survivor credit", n, err)
					}
				}
				for _, id := range ids {
					if _, err := c.WriteStream(id, []byte{2}, false); !errors.Is(err, stream.ErrWouldBlock) {
						t.Fatal(err)
					}
				}
				connections, streams := 0, 0
				for _, item := range survivor.queue {
					switch f := item.frame.(type) {
					case wire.DataBlockedFrame:
						if f.Limit != 2 {
							t.Fatal(f)
						}
						connections++
					case wire.StreamDataBlockedFrame:
						if f.Limit != 1 {
							t.Fatal(f)
						}
						streams++
					}
				}
				if connections != 1 || streams != 2 || c.streams.dataBlockedCount != 2 {
					t.Fatal("new blocking generation missing on survivor")
				}
			})
		}
	}
}
