package transport

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func TestEngineCongestionWindowAndPTO(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(v), func(t *testing.T) {
			client, _ := testPair(t, v)
			now := time.Unix(100, 0)
			for i := 0; i < 30; i++ {
				if err := client.enqueue(initialSpace, wire.PingFrame{}, 1); err != nil {
					t.Fatal(err)
				}
			}
			flight := drain(t, client, now)
			if len(flight) != 10 {
				t.Fatal("initial window packet count", len(flight))
			}
			window, bytes := client.CongestionStats()
			if window != 12000 || bytes != 12000 {
				t.Fatal(window, bytes)
			}
			// A pending padded Initial ACK must not hide the PTO probe behind a
			// congestion-blocked packet. Unlike pure ACK packets, padding is in flight.
			client.spaces[initialSpace].received.Record(0, now)
			client.spaces[initialSpace].ackPending = true
			deadline := client.RecoveryDeadline()
			if err := client.HandleTimeout(deadline); err != nil {
				t.Fatal(err)
			}
			probes := drain(t, client, deadline)
			if len(probes) != 1 {
				t.Fatal("PTO allowance must permit exactly one packet", len(probes))
			}
			window, bytes = client.CongestionStats()
			if window != 12000 || bytes != 13200 {
				t.Fatal("PTO changed window or missed accounting", window, bytes)
			}
			if err := client.handleAck(initialSpace, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 10}}}, deadline.Add(10*time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			if _, bytes = client.CongestionStats(); bytes != 0 {
				t.Fatal("ACK did not release flight", bytes)
			}
			if b, err := client.NextDatagram(deadline.Add(10 * time.Millisecond)); err != nil || len(b) == 0 {
				t.Fatal("ACK did not unblock sender", err)
			}
		})
	}
}

func TestEnginePureACKBypassesCongestion(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	now := time.Unix(200, 0)
	for i := 0; i < 1000; i++ {
		if err := client.enqueue(applicationSpace, wire.PingFrame{}, 1); err != nil {
			t.Fatal(err)
		}
	}
	// There are many small short-header packets; stop at the controller gate.
	for i := 0; i < 2000; i++ {
		b, err := client.NextDatagram(now)
		if err != nil {
			t.Fatal(err)
		}
		if len(b) == 0 {
			break
		}
		if i == 1999 {
			t.Fatal("window never blocked")
		}
	}
	_, before := client.CongestionStats()
	s := &client.spaces[applicationSpace]
	s.received.Record(100, now)
	s.ackPending = true
	b, err := client.NextDatagram(now)
	if err != nil || len(b) == 0 {
		t.Fatal("congestion blocked pure ACK", err)
	}
	if _, after := client.CongestionStats(); after != before {
		t.Fatal("pure ACK increased flight", before, after)
	}
}

func TestPersistentCongestionRefreshesPathRTT(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	now := time.Unix(200, 0)
	// Use a fresh path ledger with an established RTT sample.
	ledger, _ := recovery.NewSent(20)
	ledger.RTT.Update(10*time.Millisecond, 0, 0, true)
	client.recovery = ledger
	for i := 0; i < 4; i++ {
		if err := ledger.OnSent(recovery.Application, recovery.Packet{Number: uint64(i), SentAt: now.Add(time.Duration(i) * time.Second), Bytes: 1200, InFlight: true, AckEliciting: true}); err != nil {
			t.Fatal(err)
		}
	}
	ev, err := ledger.OnACK(recovery.Application, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 3, Largest: 3}}}, now.Add(3*time.Second+10*time.Millisecond), 0, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	client.onCongestionLoss(ev.Lost, now.Add(4*time.Second))
	if client.congestion.Window() != 2400 {
		t.Fatal("window not collapsed")
	}
	ledger.RTT.Update(100*time.Millisecond, 0, 0, true)
	if ledger.RTT.Minimum != 100*time.Millisecond {
		t.Fatal("path minimum not refreshed")
	}
}
