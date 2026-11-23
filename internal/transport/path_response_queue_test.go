package transport

import (
	"github.com/getlantern/photon/internal/wire"
	"testing"
	"time"
)

func TestSaturatedPathResponses(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		c, peer := sendingPathPair(t, version)
		c.multipath.paths[1].application.budget.Validate()
		a := peer.multipath.paths[1].application
		peer.queuedBytes = maxSendBytes
		now := time.Unix(101, 0)
		var last []byte
		for i := 0; i < 3; i++ {
			token := [8]byte{byte(i + 1)}
			if err := c.queueFrameOnPath(1, wire.PathChallengeFrame{Data: token}); err != nil {
				t.Fatal(err)
			}
			b, err := c.nextPacketOnPath(1, now)
			if err != nil || b == nil {
				t.Fatal(err)
			}
			last = append([]byte(nil), b...)
			if err := peer.handleMultipathPacket(b, now); err != nil {
				t.Fatal(err)
			}
			now = now.Add(time.Millisecond)
		}
		if err := peer.handleMultipathPacket(last, now); err != nil {
			t.Fatal(err)
		}
		if a.controlResponses.count != 3 || len(a.space.queue) != 0 || peer.queuedBytes != maxSendBytes {
			t.Fatal("response loss, duplicate or application charge")
		}
		if b, err := peer.nextPacketOnPath(1, now); err != nil || b != nil || a.controlResponses.count != 3 || a.space.nextPN != 0 {
			t.Fatal("blocked response committed", err)
		}
		if err := peer.queuePathProbe(1); err != nil || a.controlResponses.count != 3 || len(a.space.queue) != 0 {
			t.Fatal("PTO failed to reuse response slot", err)
		}
		a.budget.Validate()
		for i := 0; i < 3; i++ {
			b, err := peer.nextPacketOnPath(1, now)
			if err != nil || len(b) != 1200 {
				t.Fatal("expanded response", err)
			}
			decoded, err := c.decodeMultipathPacket(b, now)
			if err != nil || decoded == nil {
				t.Fatal(err)
			}
			response, ok := decoded.frames[0].(wire.PathResponseFrame)
			if !ok || response.Data != ([8]byte{byte(i + 1)}) {
				t.Fatal("response token order")
			}
			now = now.Add(time.Millisecond)
		}
		if a.controlResponses.count != 0 || !a.space.ackPending || peer.queuedBytes != maxSendBytes {
			t.Fatal("response completion consumed ACK state or memory charge")
		}
	}
}

func TestPathResponseReserveBound(t *testing.T) {
	var q pathResponseQueue
	for i := 0; i < len(q.frames); i++ {
		if !q.push(wire.PathResponseFrame{Data: [8]byte{byte(i)}}) {
			t.Fatal("early overflow")
		}
	}
	before := q
	if q.push(wire.PathResponseFrame{}) || q != before {
		t.Fatal("overflow overwrote pending response")
	}
}

func TestPathResponsesPrecedeNewChallenges(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		c, peer := sendingPathPair(t, version)
		a := c.multipath.paths[1].application
		a.validation.state = PathValidated
		a.budget.Validate()
		if err := c.queueFrameOnPath(1, wire.StreamFrame{StreamID: 0, Data: []byte("backlog")}); err != nil {
			t.Fatal(err)
		}
		for i := byte(1); i <= 3; i++ {
			if err := c.queuePathResponse(1, wire.PathResponseFrame{Data: [8]byte{i}}); err != nil {
				t.Fatal(err)
			}
		}
		if err := c.queueChallengeOnPath(1); err != nil {
			t.Fatal(err)
		}
		before := c.queuedBytes
		now := time.Unix(101, 0)
		for i := byte(1); i <= 3; i++ {
			b, err := c.nextPacketOnPath(1, now)
			if err != nil || len(b) != 1200 {
				t.Fatal("response send", len(b), err)
			}
			decoded, err := peer.decodeMultipathPacket(b, now)
			if err != nil || decoded == nil {
				t.Fatal(err)
			}
			f, ok := decoded.frames[0].(wire.PathResponseFrame)
			if !ok || f.Data != ([8]byte{i}) {
				t.Fatal("response priority/order")
			}
			now = now.Add(time.Millisecond)
		}
		if len(a.space.queue) != 2 {
			t.Fatal("lost queued work")
		}
		if _, ok := a.space.queue[0].frame.(wire.PathChallengeFrame); !ok {
			t.Fatal("challenge displaced")
		}
		if _, ok := a.space.queue[1].frame.(wire.StreamFrame); !ok {
			t.Fatal("stream displaced")
		}
		if c.queuedBytes != before-3*32 {
			t.Fatal("response charge release", c.queuedBytes, before)
		}
	}
}

func TestSchedulerResponseBeforeACKAndPTO(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		c, peer := sendingPathPair(t, v)
		now := time.Unix(101, 0)
		for _, id := range []uint64{0, 1} {
			c.multipath.paths[id].application.budget.Validate()
			peer.multipath.paths[id].application.budget.Validate()
			var f wire.Frame = wire.PingFrame{}
			if id == 1 {
				f = wire.PathChallengeFrame{Data: [8]byte{42}}
			}
			if err := peer.queueFrameOnPath(id, f); err != nil {
				t.Fatal(err)
			}
			b, err := peer.nextPacketOnPath(id, now)
			if err != nil || b == nil {
				t.Fatal(err)
			}
			if err := c.handleMultipathPacket(b, now); err != nil {
				t.Fatal(err)
			}
		}
		a := c.multipath.paths[1].application
		original := a.space.queue[0]
		a.probePending = true
		if err := c.queuePathProbe(1); err != nil {
			t.Fatal(err)
		}
		if len(a.space.queue) != 1 || a.space.queue[0] != original {
			t.Fatal("PTO displaced response")
		}
		id, b, err := c.nextMultipathPacket(now.Add(time.Millisecond))
		if err != nil || id != 1 || len(b) != 1200 {
			t.Fatal("response priority", id, len(b), err)
		}
		decoded, err := peer.decodeMultipathPacket(b, now)
		if err != nil || decoded == nil {
			t.Fatal(err)
		}
		f, ok := decoded.frames[0].(wire.PathResponseFrame)
		if !ok || f.Data != ([8]byte{42}) {
			t.Fatal("response token")
		}
		if !a.space.ackPending || !c.multipath.paths[0].application.space.ackPending || a.probePending {
			t.Fatal("ACK/PTO state")
		}
		id, b, err = c.nextMultipathPacket(now.Add(2 * time.Millisecond))
		if err != nil || id != 0 || b == nil {
			t.Fatal("deferred ACK", id, err)
		}
	}
}

func TestBlockedResponseStillSendsACK(t *testing.T) {
	for _, paced := range []bool{false, true} {
		c, peer := sendingPathPair(t, wire.Version2)
		a := c.multipath.paths[1].application
		a.budget.Validate()
		peer.multipath.paths[1].application.budget.Validate()
		now := time.Unix(101, 0)
		if err := peer.queueFrameOnPath(1, wire.PathChallengeFrame{Data: [8]byte{7}}); err != nil {
			t.Fatal(err)
		}
		b, err := peer.nextPacketOnPath(1, now)
		if err != nil || b == nil {
			t.Fatal(err)
		}
		if err := c.handleMultipathPacket(b, now); err != nil {
			t.Fatal(err)
		}
		original := a.space.queue[0]
		if paced {
			a.pacer.Delay(now, a.congestion.Window(), a.sent.RTT.Smoothed, 1200)
			a.pacer.Sent(int(a.congestion.Window()) * 2)
		} else {
			a.sent.BytesInFlight = a.congestion.Window()
		}
		b, err = c.nextPacketOnPath(1, now)
		if err != nil || b == nil {
			t.Fatal("blocked response suppressed ACK", paced, err)
		}
		decoded, err := peer.decodeMultipathPacket(b, now)
		if err != nil || decoded == nil {
			t.Fatal(err)
		}
		if _, ok := decoded.frames[0].(wire.PathAckFrame); !ok {
			t.Fatal("not ACK", paced)
		}
		if a.space.ackPending || len(a.space.queue) != 1 || a.space.queue[0] != original || !original.queued {
			t.Fatal("fallback lost response")
		}
		if paced && a.pacingAt.IsZero() {
			t.Fatal("lost response pacing deadline")
		}
	}
}
