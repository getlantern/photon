package transport

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func TestEnginePacingDeadlineAndACKBypass(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	now := time.Unix(200, 0)
	client.pacer = recovery.Pacer{}
	client.pacer.Delay(now, client.congestion.Window(), client.recovery.RTT.Smoothed, 12000)
	client.pacer.Sent(12000)
	client.enqueue(applicationSpace, wire.PingFrame{}, 1)
	if b, err := client.NextDatagram(now); err != nil || len(b) != 0 {
		t.Fatal("paced data escaped", err)
	}
	deadline := client.RecoveryDeadline()
	if !deadline.After(now) {
		t.Fatal("missing pacing wakeup")
	}
	// Pure ACKs can leave immediately even while reliable data is paced.
	client.spaces[applicationSpace].received.Record(1000, now)
	client.spaces[applicationSpace].ackPending = true
	if b, err := client.NextDatagram(now); err != nil || len(b) == 0 {
		t.Fatal("ACK paced", err)
	}
	client.NextDatagram(now)
	if err := client.HandleTimeout(deadline); err != nil {
		t.Fatal(err)
	}
	if b, err := client.NextDatagram(deadline); err != nil || len(b) == 0 {
		t.Fatal("pacing wakeup failed", err)
	}
}
