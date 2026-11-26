package transport

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestMultipathPTOProbe(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := sendingPathPair(t, v)
		// A PTO mustn't cut the window, which only NewReno would otherwise do.
		useNewReno(t, client)
		now := time.Unix(101, 0)
		app := client.multipath.paths[1].application
		app.budget.Validate()
		if err := client.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
			t.Fatal(err)
		}
		if b, err := client.nextPacketOnPath(1, now); err != nil || b == nil {
			t.Fatal(err)
		}
		deadline := app.timer.Deadline()
		if deadline.IsZero() {
			t.Fatal("missing PTO")
		}
		if err := client.expireMultipathRecovery(deadline.Add(-time.Nanosecond)); err != nil {
			t.Fatal(err)
		}
		if app.probePending {
			t.Fatal("early PTO")
		}
		flight, window := app.sent.BytesInFlight, app.congestion.Window()
		if err := client.expireMultipathRecovery(deadline); err != nil {
			t.Fatal(err)
		}
		if !app.probePending || app.timer.Backoff() != 1 || app.sent.BytesInFlight != flight || app.congestion.Window() != window {
			t.Fatal("PTO declared loss or did not request probe")
		}
		// Repeated callbacks cannot double backoff or enqueue duplicate probes.
		if err := client.expireMultipathRecovery(deadline); err != nil {
			t.Fatal(err)
		}
		if len(app.space.queue) != 1 || app.timer.Backoff() != 1 {
			t.Fatal("duplicate PTO")
		}
		app.sent.BytesInFlight = window // Force the congestion gate closed.
		beforeZero := client.spaces[applicationSpace].nextPN
		id, raw, err := client.nextMultipathPacket(deadline)
		if err != nil || id != 1 || raw == nil {
			t.Fatal("probe blocked by congestion", id, err)
		}
		if app.probePending || !app.timer.Deadline().After(deadline) || client.spaces[applicationSpace].nextPN != beforeZero {
			t.Fatal("probe accounting/rearm")
		}
		if err := server.handleMultipathPacket(raw, deadline); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMultipathPTOSuppressedByCreditAndAbandonment(t *testing.T) {
	client, _ := sendingPathPair(t, wire.Version1)
	now := time.Unix(101, 0)
	app := client.multipath.paths[1].application
	app.budget.Validate()
	if err := client.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.nextPacketOnPath(1, now); err != nil {
		t.Fatal(err)
	}
	deadline := app.timer.Deadline()
	*app.budget = Amplification{}
	if err := client.expireMultipathRecovery(deadline); err != nil {
		t.Fatal(err)
	}
	if app.probePending || app.timer.Backoff() != 0 || !app.timer.Deadline().IsZero() {
		t.Fatal("PTO without credit")
	}
	app.budget.Validate()
	if _, err := client.multipath.beginAbandon(1); err != nil {
		t.Fatal(err)
	}
	if err := client.expireMultipathRecovery(deadline); err != nil {
		t.Fatal(err)
	}
	if app.probePending || app.timer.Backoff() != 0 {
		t.Fatal("PTO on abandoned path")
	}
}

func TestMultipathTimeThresholdLoss(t *testing.T) {
	client, _ := sendingPathPair(t, wire.Version1)
	now := time.Unix(101, 0)
	app := client.multipath.paths[1].application
	// NewReno cuts the window on loss; BBR, as draft-07, doesn't.
	useNewReno(t, client)
	app.budget.Validate()
	for i := 0; i < 2; i++ {
		if err := client.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
			t.Fatal(err)
		}
		if b, err := client.nextPacketOnPath(1, now); err != nil || b == nil {
			t.Fatal(err)
		}
	}
	maxDelay := time.Duration(client.peerParams.MaxAckDelay) * time.Millisecond
	events, err := client.multipath.receiveACK(wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 1, Largest: 1}}}}, now.Add(time.Millisecond), 0, maxDelay, true)
	if err != nil || len(events.Lost) != 0 {
		t.Fatal("premature loss", events, err)
	}
	client.armPathRecovery(1, now.Add(time.Millisecond))
	deadline := app.timer.Deadline()
	window := app.congestion.Window()
	if deadline.IsZero() || !deadline.After(now.Add(time.Millisecond)) {
		t.Fatal("missing loss timer")
	}
	if err := client.expireMultipathRecovery(deadline); err != nil {
		t.Fatal(err)
	}
	if app.sent.BytesInFlight != 0 || app.congestion.Window() >= window || app.probePending || len(app.space.queue) != 1 {
		t.Fatal("loss did not requeue and reduce congestion")
	}
	if app.space.queue[0] != app.space.sent[0] {
		t.Fatal("lost retransmission lost logical identity")
	}
}

func TestMultipathProbeSurvivesLateACK(t *testing.T) {
	client, _ := sendingPathPair(t, wire.Version1)
	now := time.Unix(101, 0)
	app := client.multipath.paths[1].application
	app.budget.Validate()
	if err := client.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
		t.Fatal(err)
	}
	if b, err := client.nextPacketOnPath(1, now); err != nil || b == nil {
		t.Fatal(err)
	}
	deadline := app.timer.Deadline()
	if err := client.expireMultipathRecovery(deadline); err != nil {
		t.Fatal(err)
	}
	ack := wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}}}
	_, err := client.multipath.receiveReliableACK(ack, deadline, 0, 25*time.Millisecond, true, func(item *sendItem) error { client.queuedBytes -= item.bytes; return nil })
	if err != nil {
		t.Fatal(err)
	}
	// Credit disappearing while a probe waits cannot authorize a transmission.
	*app.budget = Amplification{}
	if b, err := client.nextPacketOnPath(1, deadline); err != nil || b != nil {
		t.Fatal("probe bypassed credit", err)
	}
	if !app.probePending || app.space.nextPN != 1 {
		t.Fatal("blocked probe consumed")
	}
	app.budget.Validate()
	if b, err := client.nextPacketOnPath(1, deadline); err != nil || b == nil {
		t.Fatal("late ACK stranded probe", err)
	}
	if app.probePending || app.space.nextPN != 2 {
		t.Fatal("replacement probe accounting")
	}
}
