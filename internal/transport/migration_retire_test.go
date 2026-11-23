package transport

import (
	"errors"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestSelectiveMigrationCIDRetirement(t *testing.T) {
	p := migrationPeerIDs(t)
	candidate := &migrationCandidate{}
	seq, _ := p.reserveMigrationID(candidate)
	p.sentMigrationID(candidate, seq)
	if err := p.retireReserved(candidate, seq); err != nil {
		t.Fatal(err)
	}
	if p.current != 0 || p.retireBefore != 0 {
		t.Fatal("selective retirement disturbed established CID")
	}
	frame := wire.NewConnectionIDFrame{Sequence: seq, ConnectionID: []byte{byte(seq)}}
	for range 2 {
		retired, err := p.add(frame)
		if err != nil || len(retired) != 1 || retired[0] != seq {
			t.Fatal(retired, err)
		}
		if _, ok := p.active[seq]; ok {
			t.Fatal("delayed advertisement revived CID")
		}
	}
	if _, err := p.add(wire.NewConnectionIDFrame{Sequence: 4, RetirePriorTo: 2, ConnectionID: []byte{4}}); err != nil {
		t.Fatal(err)
	}
	if len(p.retired) != 0 {
		t.Fatal("retirement floor failed to collect sparse tombstone")
	}
}

func TestMigrationRetirementQueueAndEncryptedDelivery(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := sendingPathPair(t, version)
		peers := client.multipath.paths[1].peer
		oldCurrent := peers.current
		candidate := &migrationCandidate{}
		seq, ok := peers.reserveMigrationID(candidate)
		if !ok || !peers.sentMigrationID(candidate, seq) {
			t.Fatal("missing candidate reservation")
		}
		original := peers.active[seq]
		beforeQueue := len(client.spaces[applicationSpace].queue)
		beforeBytes := client.queuedBytes
		if err := client.retireMigrationReservations(1, candidate); err != nil || len(client.spaces[applicationSpace].queue) != beforeQueue {
			t.Fatal("current candidate retired", err)
		}
		client.queuedBytes = maxSendBytes - 31
		if err := client.retireMigrationReservations(1, nil); !errors.Is(err, ErrSendBuffer) {
			t.Fatal(err)
		}
		if _, ok := peers.migrationID(candidate, seq); !ok || len(peers.retired) != 0 || len(client.spaces[applicationSpace].queue) != beforeQueue || client.Err() != nil {
			t.Fatal("queue rejection mutated reservation")
		}
		client.queuedBytes = beforeBytes
		// Normal candidate output servicing collects reservations after cancellation.
		now := time.Unix(101, 0)
		if raw, _, err := client.nextMigrationProbe(1, now); err != nil || raw != nil {
			t.Fatal("canceled reservation produced probe", err)
		}
		if peers.current != oldCurrent || len(client.spaces[applicationSpace].queue) != beforeQueue+1 || client.queuedBytes != beforeBytes+32 {
			t.Fatal("retirement did not commit atomically")
		}
		if _, ok := peers.active[seq]; ok {
			t.Fatal("retired CID still active")
		}
		if err := client.retireMigrationReservations(1, nil); err != nil || client.queuedBytes != beforeBytes+32 {
			t.Fatal("duplicate retirement queued", err)
		}
		delivered := false
		for range 20 {
			raw, err := client.nextPacketOnPath(0, now)
			if err != nil {
				t.Fatal(err)
			}
			if raw != nil {
				if err := server.handleMultipathPacket(raw, now); err != nil {
					t.Fatal(err)
				}
				if _, ok := server.multipath.paths[1].local.active[seq]; !ok {
					delivered = true
					break
				}
			}
			now = now.Add(time.Millisecond)
		}
		if !delivered {
			t.Fatal("encrypted retirement not delivered")
		}
		retired, err := client.multipath.receiveNew(wire.PathNewConnectionIDFrame{PathID: 1, NewConnectionIDFrame: wire.NewConnectionIDFrame{Sequence: seq, ConnectionID: original.cid, StatelessResetToken: original.token}})
		if err != nil || len(retired) != 1 || retired[0].Sequence != seq {
			t.Fatal("old advertisement not re-retired", retired, err)
		}
	}
}
