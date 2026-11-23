package transport

import (
	"github.com/getlantern/photon/internal/wire"
	"testing"
)

func migrationPeerIDs(t *testing.T) *peerIDs {
	t.Helper()
	p := newPeerIDs([]byte("initial"), 4)
	for seq := uint64(1); seq <= 3; seq++ {
		if _, err := p.add(wire.NewConnectionIDFrame{Sequence: seq, ConnectionID: []byte{byte(seq)}}); err != nil {
			t.Fatal(err)
		}
	}
	return p
}

func TestMigrationCIDReservationLifecycle(t *testing.T) {
	p := migrationPeerIDs(t)
	candidate := &migrationCandidate{}
	seq, ok := p.reserveMigrationID(candidate)
	if !ok || seq != 1 || p.current != 0 || p.active[seq].used {
		t.Fatal("reservation changed established CID or use state")
	}
	if again, ok := p.reserveMigrationID(candidate); !ok || again != seq {
		t.Fatal("reservation not idempotent")
	}
	if next, ok := p.nextUnused(); !ok || next != 2 {
		t.Fatal("rotation selected reserved CID")
	}
	if _, ok := p.migrationID(&migrationCandidate{}, seq); ok {
		t.Fatal("foreign candidate owns CID")
	}
	if p.commitMigrationID(candidate, seq) {
		t.Fatal("unsent unvalidated CID committed")
	}
	if p.cancelMigrationID(candidate, seq) || p.active[seq].reservation != nil {
		t.Fatal("unsent cancellation consumed CID")
	}
	seq, ok = p.reserveMigrationID(candidate)
	if !ok || seq != 1 || !p.sentMigrationID(candidate, seq) {
		t.Fatal("probe use failed")
	}
	if !p.cancelMigrationID(candidate, seq) || p.active[seq].reservation != candidate {
		t.Fatal("sent cancellation exposed CID on old tuple")
	}
	if p.commitMigrationID(candidate, seq) {
		t.Fatal("unvalidated commit")
	}
	candidate.validation.state = PathValidated
	old := p.active[0]
	old.used = true
	p.active[0] = old
	if !p.commitMigrationID(candidate, seq) || p.current != seq || p.active[seq].reservation != nil || p.active[0].reservation == nil {
		t.Fatal("commit failed to isolate old tuple CID")
	}
	if p.commitMigrationID(candidate, seq) {
		t.Fatal("double commit accepted")
	}
}

func TestMigrationCIDReservationSurvivesDuplicateAndRetirement(t *testing.T) {
	p := migrationPeerIDs(t)
	candidate := &migrationCandidate{}
	seq, _ := p.reserveMigrationID(candidate)
	p.sentMigrationID(candidate, seq)
	if _, err := p.add(wire.NewConnectionIDFrame{Sequence: seq, ConnectionID: []byte{byte(seq)}}); err != nil {
		t.Fatal(err)
	}
	if id, ok := p.migrationID(candidate, seq); !ok || !id.used {
		t.Fatal("duplicate erased ownership")
	}
	// Peer retirement must revoke the reservation, not revive or silently replace it.
	if _, err := p.add(wire.NewConnectionIDFrame{Sequence: 4, RetirePriorTo: 2, ConnectionID: []byte{4}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.migrationID(candidate, seq); ok || p.sentMigrationID(candidate, seq) || p.commitMigrationID(candidate, seq) || p.cancelMigrationID(candidate, seq) {
		t.Fatal("retired reservation survived")
	}
}

func TestMigrationReservedCIDNeverBecomesAutomaticCurrent(t *testing.T) {
	p := newPeerIDs([]byte("initial"), 2)
	// MaxVarint is a valid sequence and cannot serve as the no-current sentinel.
	frame := wire.NewConnectionIDFrame{Sequence: wire.MaxVarint, ConnectionID: []byte("last")}
	if _, err := p.add(frame); err != nil {
		t.Fatal(err)
	}
	candidate := &migrationCandidate{}
	seq, ok := p.reserveMigrationID(candidate)
	if !ok || seq != wire.MaxVarint {
		t.Fatal("missing reservation")
	}
	frame.RetirePriorTo = wire.MaxVarint
	if _, err := p.add(frame); err != nil {
		t.Fatal(err)
	}
	if _, ok := p.active[p.current]; ok {
		t.Fatal("reserved CID selected for established address")
	}
	if _, ok := p.migrationID(candidate, seq); !ok {
		t.Fatal("reservation lost")
	}
	if _, ok := p.reserveMigrationID(&migrationCandidate{}); ok {
		t.Fatal("CID has two owners")
	}
	if p.cancelMigrationID(candidate, seq) || p.current != seq {
		t.Fatal("unsent cancellation failed to restore ordinary CID")
	}
}
