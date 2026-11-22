package transport

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestReceivedECN(t *testing.T) {
	now := time.Unix(100, 0)
	r, _ := NewReceived(2)
	record := func(pn uint64, mark ECN, want Receipt) {
		t.Helper()
		got, err := r.RecordECN(pn, now, mark)
		if err != nil || got != want {
			t.Fatal(pn, got, err)
		}
	}
	record(0, ECNUnknown, NewPacket)
	ack, _ := r.Ack(now, 0)
	if ack.ECN != nil {
		t.Fatal("invented ECN metadata")
	}
	record(1, ECNNotECT, NewPacket)
	ack, _ = r.Ack(now, 0)
	if ack.ECN == nil || *ack.ECN != [3]uint64{} {
		t.Fatal("observed Not-ECT feedback")
	}
	record(2, ECNECT0, NewPacket)
	record(3, ECNECT1, NewPacket)
	record(4, ECNCE, NewPacket)
	record(4, ECNCE, DuplicatePacket)
	record(2, ECNCE, DuplicatePacket)
	old, _ := r.Ack(now, 0)
	if old.ECN == nil || *old.ECN != [3]uint64{1, 1, 1} {
		t.Fatal("counts", old.ECN)
	}
	// Sparse packets evict the first range. Counts describe all processed
	// packets, not merely those still retained in this ACK's ranges.
	record(10, ECNECT0, NewPacket)
	record(20, ECNCE, NewPacket)
	record(0, ECNCE, ForgottenPacket)
	record(21, ECNUnknown, NewPacket)
	latest, _ := r.Ack(now, 0)
	if *latest.ECN != [3]uint64{2, 1, 2} || *old.ECN != [3]uint64{1, 1, 1} {
		t.Fatal("eviction or snapshot alias")
	}
	*latest.ECN = [3]uint64{}
	again, _ := r.Ack(now, 0)
	if *again.ECN != [3]uint64{2, 1, 2} {
		t.Fatal("ACK mutated receive counters")
	}
	if _, err := again.Append(nil); err != nil {
		t.Fatal("ACK ECN wire encoding", err)
	}
}

func TestReceivedECNSpacesAndInvalidMetadata(t *testing.T) {
	now := time.Unix(100, 0)
	spaces := make([]*Received, 3)
	for i := range spaces {
		spaces[i], _ = NewReceived(2)
		// A coalesced Initial, Handshake and application packet share a CE mark.
		if _, err := spaces[i].RecordECN(0, now, ECNCE); err != nil {
			t.Fatal(err)
		}
	}
	spaces[0].RecordECN(1, now, ECNECT0)
	for i, r := range spaces {
		ack, _ := r.Ack(now, 0)
		want := [3]uint64{0, 0, 1}
		if i == 0 {
			want[0] = 1
		}
		if *ack.ECN != want {
			t.Fatal("cross-space counters", i, *ack.ECN)
		}
		before := r.Expected()
		if _, err := r.RecordECN(100, now, ECN(4)); err == nil || r.Expected() != before {
			t.Fatal("invalid metadata mutated range")
		}
		if _, err := r.RecordECN(wire.MaxVarint+1, now, ECNCE); err == nil {
			t.Fatal("invalid packet number counted")
		}
		after, _ := r.Ack(now, 0)
		if *after.ECN != want {
			t.Fatal("invalid input changed counters")
		}
	}
}
