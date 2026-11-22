package transport

import (
	"errors"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestMultipathKeyUpdateSpacing(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := testPair(t, v)
		drivePair(t, client, server, false, false)
		attachSlowPath(t, client)
		now := time.Unix(101, 0)
		if err := client.InitiateKeyUpdateAt(now); err != nil {
			t.Fatal("first update", err)
		}
		peerNext, err := server.writeKeys(applicationSpace).Next()
		if err != nil {
			t.Fatal(err)
		}
		raw := pathPhasePacket(t, client, peerNext, 0, true, 1000)
		if _, _, _, err = client.openApplicationPath(0, raw, 1+len(client.config.LocalCID), 1000, now); err != nil {
			t.Fatal(err)
		}
		if len(drain(t, client, now)) == 0 {
			t.Fatal("no current-phase transmission")
		}
		pn := client.spaces[applicationSpace].nextPN - 1
		ack := wire.AckFrame{Ranges: []wire.AckRange{{Smallest: pn, Largest: pn}}}
		ackAt := now.Add(10 * time.Millisecond)
		if err := client.handleAck(applicationSpace, ack, ackAt); err != nil {
			t.Fatal(err)
		}
		want := ackAt.Add(3 * 1425 * time.Millisecond)
		if !client.applicationKeys.updateNotBefore.Equal(want) {
			t.Fatalf("deadline %v want %v", client.applicationKeys.updateNotBefore, want)
		}
		if err := client.handleAck(applicationSpace, ack, ackAt.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if !client.applicationKeys.updateNotBefore.Equal(want) {
			t.Fatal("duplicate ACK restarted wait")
		}
		before := client.applicationKeys
		queued := client.queuedBytes
		for _, early := range []time.Time{time.Time{}, ackAt, want.Add(-time.Nanosecond)} {
			if err := client.InitiateKeyUpdateAt(early); !errors.Is(err, errKeyUpdate) {
				t.Fatal("early update accepted", err)
			}
			if client.applicationKeys != before || client.queuedBytes != queued {
				t.Fatal("rejected update mutated state")
			}
		}
		if err := client.InitiateKeyUpdateAt(want); err != nil {
			t.Fatal("exact deadline rejected", err)
		}
		if client.applicationKeys.phase || client.applicationKeys.writeAcknowledged || !client.applicationKeys.updateNotBefore.IsZero() {
			t.Fatal("new generation inherited acknowledgment/deadline")
		}
	}
}

func TestMultipathACKSpacingUsesNamedPath(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	secondary := attachSlowPath(t, client)
	client.applicationKeys.updated = true
	client.applicationKeys.writeAcknowledged = false
	secondary.keyNumbers.firstSent = 5
	now := time.Unix(101, 0)
	frame := wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 4, Largest: 4}}}}
	client.noteKeyPhaseWriteACK(frame, now)
	if !client.applicationKeys.updateNotBefore.IsZero() {
		t.Fatal("previous-phase ACK started wait")
	}
	frame.Ranges[0] = wire.AckRange{Smallest: 5, Largest: 5}
	client.noteKeyPhaseWriteACK(frame, now)
	if !client.applicationKeys.updateNotBefore.Equal(now.Add(3 * 1425 * time.Millisecond)) {
		t.Fatal("PATH_ACK did not start shared wait")
	}
}
