package transport

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestMultipathACKOnSurvivingPath(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := sendingPathPair(t, v)
		now := time.Unix(101, 0)
		server.multipath.paths[1].application.budget.Validate()
		if err := server.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
			t.Fatal(err)
		}
		raw, err := server.nextPacketOnPath(1, now)
		if err != nil || raw == nil {
			t.Fatal(err)
		}
		if err := client.handleMultipathPacket(raw, now); err != nil {
			t.Fatal(err)
		}
		client.multipath.beginAbandon(1)
		ackNow(client)
		before := client.spaces[applicationSpace].nextPN
		id, ack, err := client.nextMultipathPacket(now.Add(time.Millisecond))
		if err != nil || id != 0 || ack == nil {
			t.Fatal("ACK did not use surviving path", id, err)
		}
		if client.spaces[applicationSpace].nextPN != before+1 || client.multipath.paths[1].application.space.nextPN != 0 {
			t.Fatal("ACK consumed source receive path's sending PN")
		}
		if client.multipath.paths[1].application.space.ackPending {
			t.Fatal("ACK source stayed pending after send")
		}
		if err := server.handleMultipathPacket(ack, now.Add(2*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		if server.multipath.paths[1].application.sent.BytesInFlight != 0 {
			t.Fatal("cross-path ACK did not acknowledge original packet")
		}
	}
}

func TestMultipathACKFallbackPreservesPending(t *testing.T) {
	client, _ := sendingPathPair(t, wire.Version1)
	now := time.Unix(101, 0)
	source := client.multipath.paths[1].application
	source.space.received.Record(0, now)
	source.space.ackPending = true
	// Both paths lack credit. A blocked attempt cannot consume an ACK or PN.
	client.budget = Amplification{}
	before := client.spaces[applicationSpace].nextPN
	if _, b, err := client.nextMultipathPacket(now); err != nil || b != nil {
		t.Fatal("sent without credit", err)
	}
	if !source.space.ackPending || source.space.nextPN != 0 || client.spaces[applicationSpace].nextPN != before {
		t.Fatal("blocked ACK committed")
	}
	client.budget.Validate()
	id, b, err := client.nextMultipathPacket(now)
	if err != nil || id != 0 || b == nil || source.space.ackPending {
		t.Fatal("ACK fallback", id, err)
	}
}

func TestMultipathSchedulerPreferences(t *testing.T) {
	client, _ := sendingPathPair(t, wire.Version1)
	now := time.Unix(101, 0)
	secondary := client.multipath.paths[1].application
	secondary.budget.Validate()
	for _, id := range []uint64{0, 1, 0, 1} {
		if err := client.queueFrameOnPath(id, wire.PingFrame{}); err != nil {
			t.Fatal(err)
		}
	}
	for i, want := range []uint64{0, 1, 0, 1} {
		id, b, err := client.nextMultipathPacket(now.Add(time.Duration(i) * time.Millisecond))
		if err != nil || b == nil || id != want {
			t.Fatal("round robin", id, want, err)
		}
	}
	client.multipath.paths[0].backup = true
	for _, id := range []uint64{0, 1} {
		if err := client.queueFrameOnPath(id, wire.PingFrame{}); err != nil {
			t.Fatal(err)
		}
	}
	id, b, err := client.nextMultipathPacket(now.Add(time.Second))
	if err != nil || b == nil || id != 1 {
		t.Fatal("backup preferred over available", id, err)
	}
	// Once the available path has no queued traffic, a backup queue can progress.
	id, b, err = client.nextMultipathPacket(now.Add(time.Second))
	if err != nil || b == nil || id != 0 {
		t.Fatal("backup queue starved", id, err)
	}
}

func TestIdleMultipathSchedulerAllocations(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	for _, state := range c.multipath.paths {
		state.application.space.ackPending = false
		state.application.space.queue = nil
	}
	now := time.Unix(101, 0)
	allocations := testing.AllocsPerRun(100, func() {
		if _, b, err := c.nextMultipathPacket(now); err != nil || b != nil {
			t.Fatalf("idle scheduler produced output: %v", err)
		}
	})
	if allocations != 0 {
		t.Fatalf("idle scheduler allocated %g objects", allocations)
	}
}

// A delayed ACK owed on a nonzero path wakes the owner through
// RecoveryDeadline, is not sent early, and once due leaves as a PATH_ACK on a
// surviving path after its own path is abandoned.
func TestMultipathDelayedACKOnSurvivingPath(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := sendingPathPair(t, v)
		now := time.Unix(101, 0)
		server.multipath.paths[1].application.budget.Validate()
		if err := server.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
			t.Fatal(err)
		}
		raw, err := server.nextPacketOnPath(1, now)
		if err != nil || raw == nil {
			t.Fatal(err)
		}
		if err := client.handleMultipathPacket(raw, now); err != nil {
			t.Fatal(err)
		}
		due := now.Add(client.ackDelay())
		if got := client.multipath.paths[1].application.space.ackDueAt; !got.Equal(due) {
			t.Fatal("path 1 ACK deadline", got, "want", due)
		}
		if got := client.RecoveryDeadline(); got.IsZero() || got.After(due) {
			t.Fatal("RecoveryDeadline misses path 1's delayed ACK", got, "want by", due)
		}
		if id, early, err := client.nextMultipathPacket(now); err != nil || early != nil {
			t.Fatal("ACK sent before its deadline", id, err)
		}
		client.multipath.beginAbandon(1)
		if err := client.HandleTimeout(due); err != nil {
			t.Fatal(err)
		}
		id, ack, err := client.nextMultipathPacket(due)
		if err != nil || id != 0 || ack == nil {
			t.Fatal("delayed ACK did not use surviving path", id, err)
		}
		if err := server.handleMultipathPacket(ack, due); err != nil {
			t.Fatal(err)
		}
		if server.multipath.paths[1].application.sent.BytesInFlight != 0 {
			t.Fatal("delayed cross-path ACK did not acknowledge the original packet")
		}
	}
}
