package transport

import (
	"github.com/getlantern/photon/internal/wire"
	"testing"
	"time"
)

func TestMTUFallbackRequiresExpandedResponse(t *testing.T) {
	for _, id := range []uint64{0, 1} {
		c, peer := sendingPathPair(t, wire.Version2)
		a := c.multipath.paths[id].application
		a.budget.Validate()
		peer.multipath.paths[id].application.budget.Validate()
		a.validation.state = PathValidated
		a.mtuNeedsValidation = true
		now := time.Unix(101, 0)
		if err := c.queueFrameOnPath(id, wire.StreamFrame{StreamID: 0, Data: []byte("wait")}); err != nil {
			t.Fatal(err)
		}
		raw, err := c.nextPacketOnPath(id, now)
		if err != nil || len(raw) != 1200 || a.validation.state != PathValidating {
			t.Fatal("missing baseline challenge", len(raw), err)
		}
		if err := peer.handleMultipathPacket(raw, now); err != nil {
			t.Fatal(err)
		}
		// An ordinary ACK of the challenge does not prove its challenge token.
		now = now.Add(time.Millisecond)
		peer.multipath.paths[1-id].application.budget.Validate()
		ack, err := peer.nextPacketOnPathACK(1-id, id, now)
		if err != nil || ack == nil {
			t.Fatal(err)
		}
		if err := c.handleMultipathPacket(ack, now); err != nil {
			t.Fatal(err)
		}
		if !a.mtuNeedsValidation {
			t.Fatal("packet ACK replaced path-response proof")
		}
		if b, err := c.nextPacketOnPath(id, now); err != nil || b != nil {
			t.Fatal("data bypassed baseline validation", err)
		}
		now = now.Add(time.Millisecond)
		peer.multipath.paths[id].application.budget.Validate()
		response, err := peer.nextPacketOnPath(id, now)
		if err != nil || response == nil {
			t.Fatal(err)
		}
		if err := c.handleMultipathPacket(response, now); err != nil {
			t.Fatal(err)
		}
		if a.mtuNeedsValidation || a.validation.state != PathValidated {
			t.Fatal("matching response did not validate baseline")
		}
	}
}

func TestMTUFallbackBaselineFailureAbandonsPath(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	a := c.multipath.paths[1].application
	a.budget.Validate()
	a.validation.state = PathValidated
	a.mtuNeedsValidation = true
	if err := c.queueFrameOnPath(1, wire.StreamFrame{StreamID: 0, Data: []byte("survive")}); err != nil {
		t.Fatal(err)
	}
	item := a.space.queue[0]
	now := time.Unix(101, 0)
	b, err := c.nextPacketOnPath(1, now)
	if err != nil || len(b) != 1200 {
		t.Fatal(err)
	}
	// A link below the baseline drops the expanded challenge. Expiry follows
	// the existing path-failure policy and evacuates reliable data to path zero.
	if err := c.expireMultipathPaths(a.validation.expires); err != nil {
		t.Fatal(err)
	}
	if c.multipath.abandoned[1] == nil || !item.queued || item.acked {
		t.Fatal("failed baseline did not abandon and evacuate")
	}
	found := false
	for _, queued := range c.multipath.paths[0].application.space.queue {
		found = found || queued == item
	}
	if !found {
		t.Fatal("reliable data stranded")
	}
}

func TestMTUBaselineControlSlotAtFullQueue(t *testing.T) {
	c, peer := sendingPathPair(t, wire.Version1)
	a := c.multipath.paths[1].application
	a.validation.state = PathValidated
	a.mtuNeedsValidation = true
	c.queuedBytes = maxSendBytes
	peer.queuedBytes = maxSendBytes
	now := time.Unix(101, 0)
	// Starting via PTO must not need space for a queued PING or challenge.
	if err := c.queuePathProbe(1); err != nil {
		t.Fatal(err)
	}
	pending := a.mtuChallenge
	if pending == nil || c.queuedBytes != maxSendBytes || len(a.space.queue) != 0 {
		t.Fatal("unbounded/charged validation control")
	}
	if b, err := c.nextPacketOnPath(1, now); err != nil || b != nil || a.mtuChallenge != pending || a.space.nextPN != 0 || len(a.validation.probes) != 0 {
		t.Fatal("blocked challenge committed", err)
	}
	a.budget.Validate()
	peer.multipath.paths[1].application.budget.Validate()
	b, err := c.nextPacketOnPath(1, now)
	if err != nil || len(b) != 1200 || a.mtuChallenge != nil || len(a.validation.probes) != 1 || c.queuedBytes != maxSendBytes {
		t.Fatal("reserved challenge send", err)
	}
	// Drop the first challenge. A retry also uses the same bounded control slot.
	now = a.validation.next
	if err := c.expireValidationOnPath(1, now); err != nil {
		t.Fatal(err)
	}
	if a.mtuChallenge == nil || c.queuedBytes != maxSendBytes {
		t.Fatal("retry required queue credit")
	}
	b, err = c.nextPacketOnPath(1, now)
	if err != nil || len(b) != 1200 {
		t.Fatal("retry", err)
	}
	if err := peer.handleMultipathPacket(b, now); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3 && a.mtuNeedsValidation; i++ {
		now = now.Add(time.Millisecond)
		b, err = peer.nextPacketOnPath(1, now)
		if err != nil {
			t.Fatal(err)
		}
		if b != nil {
			if err := c.handleMultipathPacket(b, now); err != nil {
				t.Fatal(err)
			}
		}
	}
	if a.mtuNeedsValidation || a.mtuChallenge != nil || c.queuedBytes != maxSendBytes {
		t.Fatal("full-queue baseline completion failed")
	}
}
