package transport

import (
	"bytes"
	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
	"testing"
	"time"
)

func TestEncryptedStreamSplitAfterMTUFallback(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, lateOriginal := range []bool{false, true} {
			c, peer := sendingPathPair(t, version)
			for _, conn := range []*Conn{c, peer} {
				a := conn.multipath.paths[1].application
				a.validation.state = PathValidated
				a.budget.Validate()
			}
			a := c.multipath.paths[1].application
			if err := c.startPathMTUSearch(1, 4800); err != nil {
				t.Fatal(err)
			}
			now := time.Unix(101, 0)
			probe, err := c.nextPacketOnPath(1, now)
			if err != nil || len(probe) != 3000 {
				t.Fatal("size probe", err)
			}
			if err := peer.handleMultipathPacket(probe, now); err != nil {
				t.Fatal(err)
			}
			now = now.Add(time.Second)
			ack, err := peer.nextPacketOnPath(1, now)
			if err != nil || ack == nil {
				t.Fatal(err)
			}
			if err := c.handleMultipathPacket(ack, now); err != nil {
				t.Fatal(err)
			}
			if a.mtu.confirmed != 3000 {
				t.Fatal("size not authenticated")
			}
			payload := bytes.Repeat([]byte{37}, 2500)
			c.multipath.paths[0].backup = true
			streamID, err := c.OpenStream(false)
			if err != nil {
				t.Fatal(err)
			}
			if n, err := c.WriteStream(streamID, payload, true); err != nil || n != len(payload) {
				t.Fatal("large write admission", n, err)
			}
			root := a.space.queue[0]
			pn := a.space.nextPN
			now = now.Add(time.Second)
			original, err := c.nextPacketOnPath(1, now)
			if err != nil || len(original) <= 1200 || len(original) > 3000 {
				t.Fatal("large ordinary packet", len(original), err)
			}
			// The original is delayed/lost. Fallback makes the retransmission smaller.
			a.resetMTU()
			queueLostItems(a.space, a.space, []recovery.Packet{{Number: pn}})
			for i := 0; i < 6 && !root.acked; i++ {
				now = now.Add(time.Second)
				b, err := c.nextPacketOnPath(1, now)
				if err != nil || b == nil || len(b) > 1200 {
					t.Fatal("small retransmission", len(b), err)
				}
				if err := peer.handleMultipathPacket(b, now); err != nil {
					t.Fatal(err)
				}
				if lateOriginal && i == 0 {
					if err := peer.handleMultipathPacket(original, now); err != nil {
						t.Fatal(err)
					}
				}
				now = now.Add(time.Millisecond)
				ack, err := peer.nextPacketOnPath(1, now)
				if err != nil || ack == nil {
					t.Fatal("ACK", err)
				}
				if err := c.handleMultipathPacket(ack, now); err != nil {
					t.Fatal(err)
				}
			}
			if !root.acked || c.queuedBytes != 0 {
				t.Fatal("logical item not completed/released", c.queuedBytes)
			}
			if err := c.StreamAcknowledged(streamID); err != nil {
				t.Fatal("stream completion", err)
			}
			got := make([]byte, len(payload))
			n, err := peer.ReadStream(0, got)
			if n != len(payload) || !bytes.Equal(got, payload) {
				t.Fatal("reassembly mismatch", n, err)
			}
		}
	}
}

func TestSharedSplitBlockedKeepsTailAtSource(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	source := c.multipath.paths[0].application.space
	dest := c.multipath.paths[1].application
	dest.validation.state = PathValidated
	if err := c.queueFrameOnPath(0, wire.StreamFrame{StreamID: 0, Data: make([]byte, 2500), Fin: true}); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(101, 0)
	if b, err := c.nextSharedPathData(1, []uint64{0, 1}, now); err != nil || b != nil {
		t.Fatal("credit bypass", err)
	}
	if len(source.queue) != 2 || len(dest.space.queue) != 0 || dest.space.nextPN != 0 {
		t.Fatal("blocked split lost ownership")
	}
	total := 0
	for _, part := range source.queue {
		f := part.frame.(wire.StreamFrame)
		total += len(f.Data)
		if !part.queued {
			t.Fatal("source fragment lost queue flag")
		}
	}
	if total != 2500 {
		t.Fatal("tail lost")
	}
	dest.budget.Validate()
	if b, err := c.nextSharedPathData(1, []uint64{0, 1}, now); err != nil || b == nil || len(b) > 1200 {
		t.Fatal("borrowed fragment", err)
	}
	if len(source.queue) != 1 || len(source.queue[0].frame.(wire.StreamFrame).Data) != 1360 {
		t.Fatal("borrow consumed unsent tail")
	}
}
