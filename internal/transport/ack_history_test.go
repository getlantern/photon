package transport

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestSustainedPureACKOutput(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	now := time.Unix(200, 0)
	_, before := client.CongestionStats()
	for pn := uint64(1000); pn < 11000; pn++ {
		b := makePeerPacket(t, server, applicationSpace, pn, wire.PingFrame{})
		if err := client.HandleDatagram(b, now); err != nil {
			t.Fatal(pn, err)
		}
		ackNow(client) // the history, not the delay, is under test
		ack, err := client.NextDatagram(now)
		if err != nil || len(ack) == 0 {
			t.Fatal("ACK history stopped output", pn, err)
		}
	}
	if _, after := client.CongestionStats(); after != before {
		t.Fatal("pure ACK history changed congestion flight", before, after)
	}
	if err := client.enqueue(applicationSpace, wire.PingFrame{}, 1); err != nil {
		t.Fatal(err)
	}
	if b, err := client.NextDatagram(now); err != nil || len(b) == 0 {
		t.Fatal("data blocked behind ACK history", err)
	}
}
