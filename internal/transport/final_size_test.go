package transport

import (
	"fmt"
	"github.com/getlantern/photon/internal/wire"
	"testing"
)

func TestReceiveFinalSizeBeforeCompletion(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, mode := range []string{"fin", "reset", "partial"} {
			t.Run(fmt.Sprintf("%x/%s", v, mode), func(t *testing.T) {
				p := streamParameters()
				p.ResetStreamAt = true
				c, s := testPair(t, v, p)
				drivePair(t, c, s, false, false)
				id, err := c.OpenStream(false)
				if err != nil {
					t.Fatal(err)
				}
				calls := 0
				observe := func(size uint64) {
					calls++
					if size != 14 {
						t.Fatal("final size", size)
					}
				}
				if err := c.WatchReceiveFinalSize(id, observe); err != nil {
					t.Fatal(err)
				}
				var frame wire.Frame = wire.StreamFrame{StreamID: id, Offset: 10, Data: []byte("tail"), Fin: true}
				if mode == "reset" {
					frame = wire.ResetStreamFrame{StreamID: id, FinalSize: 14, ErrorCode: 7}
				}
				if mode == "partial" {
					frame = wire.ResetStreamAtFrame{StreamID: id, FinalSize: 14, ReliableSize: 8, ErrorCode: 7}
				}
				if err := c.handleStreamFrame(frame); err != nil {
					t.Fatal(err)
				}
				if calls != 1 {
					t.Fatal("missing final callback", calls)
				}
				if mode != "reset" && c.streams.objects[id].receive.Complete() {
					t.Fatal("callback waited for contiguous completion")
				}
				if err := c.handleStreamFrame(frame); err != nil || calls != 1 {
					t.Fatal("duplicate callback", err, calls)
				}
				if err := c.WatchReceiveFinalSize(id, observe); err != nil || calls != 2 {
					t.Fatal("late registration", err, calls)
				}
				if err := c.handleStreamFrame(wire.StreamFrame{StreamID: id, Offset: 15, Fin: true}); err == nil || calls != 2 {
					t.Fatal("invalid final size notified", err, calls)
				}
			})
		}
	}
}
