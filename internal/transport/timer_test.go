package transport

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestEngineInitialPTO(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(version), func(t *testing.T) {
			client, server := testPair(t, version)
			now := time.Unix(100, 0)
			first := drain(t, client, now) // The network loses the entire first flight.
			if len(first) == 0 {
				t.Fatal("missing first flight")
			}
			deadline := client.RecoveryDeadline()
			if deadline != now.Add(999*time.Millisecond) {
				t.Fatal("wrong initial PTO", deadline)
			}
			if !server.RecoveryDeadline().IsZero() {
				t.Fatal("server timer before receiving credit")
			}
			if err := client.HandleTimeout(deadline.Add(-time.Nanosecond)); err != nil {
				t.Fatal(err)
			}
			if len(drain(t, client, deadline.Add(-time.Nanosecond))) != 0 {
				t.Fatal("early probe")
			}
			if err := client.HandleTimeout(deadline); err != nil {
				t.Fatal(err)
			}
			probe := drain(t, client, deadline)
			if len(probe) != 1 || len(probe[0]) != 1200 {
				t.Fatal("expected one padded probe", len(probe))
			}
			if client.timer.Backoff() != 1 || client.RecoveryDeadline() != deadline.Add(1998*time.Millisecond) {
				t.Fatal("PTO did not back off")
			}
			if err := server.HandleDatagram(probe[0], deadline); err != nil {
				t.Fatal(err)
			}
			// Check anti-deadlock probing after all Initial packets are ACKed but
			// before the client has proof that the server validated its address.
			last := client.spaces[initialSpace].nextPN - 1
			if err := client.handleAck(initialSpace, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: last}}}, deadline.Add(time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			client.armTimer(deadline.Add(time.Millisecond))
			antiDeadlock := client.RecoveryDeadline()
			if antiDeadlock.IsZero() {
				t.Fatal("missing anti-deadlock timer")
			}
			if err := client.HandleTimeout(antiDeadlock); err != nil {
				t.Fatal(err)
			}
			if ping := drain(t, client, antiDeadlock); len(ping) != 1 || len(ping[0]) != 1200 {
				t.Fatal("missing padded anti-deadlock PING")
			}
		})
	}
}
