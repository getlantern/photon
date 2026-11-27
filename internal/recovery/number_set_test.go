package recovery

import (
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestNumberSet(t *testing.T) {
	var s numberSet
	for _, pn := range []uint64{5, 9, 2, 7, 9, 12} {
		s.add(pn)
	}
	members := func() []uint64 {
		var out []uint64
		s.covered(ack(0, wire.MaxVarint), func(pn uint64) bool { out = append(out, pn); return false })
		return out
	}
	if got := members(); !slices.Equal(got, []uint64{2, 5, 7, 9, 12}) || s.Len() != 5 {
		t.Fatal("members", got)
	}
	var visited []uint64
	f := wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 9, Largest: 20}, {Smallest: 3, Largest: 6}}}
	s.covered(f, func(pn uint64) bool { visited = append(visited, pn); return pn != 12 })
	if !slices.Equal(visited, []uint64{5, 9, 12}) {
		t.Fatal("covered visited", visited)
	}
	if got := members(); !slices.Equal(got, []uint64{2, 7, 12}) {
		t.Fatal("after removal", got)
	}
	s.remove(7)
	s.remove(7)
	s.add(5)
	if got := members(); !slices.Equal(got, []uint64{2, 5, 12}) || s.Len() != 3 {
		t.Fatal("after re-add", got, s.Len())
	}
	if s.dead != 0 && 2*s.dead >= len(s.nums) {
		t.Fatal("not compacted", s.nums, s.dead)
	}
	s.clear()
	if s.Len() != 0 || len(members()) != 0 {
		t.Fatal("clear")
	}
}

// Under random loss, delay and ECN marks, with an honest receiver whose ACKs
// report only what arrived, recovery accounts for every packet once: newly
// acknowledged or lost, never both, with late ACKs of lost packets counted as
// recovered and every covered packet's ECN mark counted exactly once.
func TestACKAccountingModel(t *testing.T) {
	for trial := range 20 {
		rng := rand.New(rand.NewPCG(uint64(trial), 7))
		r, err := NewSent(65536)
		if err != nil {
			t.Fatal(err)
		}
		now := time.Unix(100, 0)
		var next uint64
		bytes := map[uint64]int{}
		marks := map[uint64]ECNMark{}
		var received []uint64 // ascending
		var delayed []uint64
		receive := func(pn uint64) {
			i, found := slices.BinarySearch(received, pn)
			if !found {
				received = slices.Insert(received, i, pn)
			}
		}
		covered := map[uint64]bool{}
		acked, lost := map[uint64]bool{}, map[uint64]bool{}
		var ect [2]uint64
		var recovered uint64
		for range 1500 {
			for range rng.IntN(3) + 1 {
				p := Packet{Number: next, SentAt: now, Bytes: 1000 + rng.IntN(200), AckEliciting: true, InFlight: true, ECN: ECNMark(rng.IntN(3))}
				if err := r.OnSent(Application, p); err != nil {
					t.Fatal(err)
				}
				bytes[p.Number], marks[p.Number] = p.Bytes, p.ECN
				switch x := rng.IntN(100); {
				case x < 85:
					receive(next)
				case x < 95:
					delayed = append(delayed, next)
				}
				next++
			}
			if len(delayed) > 0 && rng.IntN(4) == 0 {
				i := rng.IntN(len(delayed))
				receive(delayed[i])
				delayed = slices.Delete(delayed, i, i+1)
			}
			now = now.Add(time.Millisecond)
			if len(received) == 0 || rng.IntN(3) == 0 {
				continue
			}
			// The receiver reports its latest 32 ranges.
			var f wire.AckFrame
			for i := len(received) - 1; i >= 0 && len(f.Ranges) <= 32; i-- {
				if n := len(f.Ranges); n > 0 && f.Ranges[n-1].Smallest == received[i]+1 {
					f.Ranges[n-1].Smallest = received[i]
					continue
				}
				if len(f.Ranges) == 32 {
					break
				}
				f.Ranges = append(f.Ranges, wire.AckRange{Smallest: received[i], Largest: received[i]})
			}
			events, err := r.OnACK(Application, f, now, 0, 0, true)
			if err != nil {
				t.Fatal(err)
			}
			for _, p := range events.Acked {
				if acked[p.Number] || lost[p.Number] {
					t.Fatal("acknowledged twice or after loss", p.Number)
				}
				acked[p.Number] = true
			}
			for _, p := range events.Lost {
				if acked[p.Number] || lost[p.Number] {
					t.Fatal("lost twice or after ACK", p.Number)
				}
				lost[p.Number] = true
			}
			ect[0] += events.NewlyECT[0]
			ect[1] += events.NewlyECT[1]
			recovered += events.RecoveredPackets
			for _, span := range f.Ranges {
				for pn := span.Smallest; pn <= span.Largest; pn++ {
					covered[pn] = true
				}
			}
		}
		var wantECT [2]uint64
		var wantRecovered, flight uint64
		for pn := range next {
			if covered[pn] && marks[pn] != NotECT {
				wantECT[marks[pn].index()]++
			}
			if covered[pn] && lost[pn] {
				wantRecovered++
			}
			if !acked[pn] && !lost[pn] {
				flight += uint64(bytes[pn])
			}
		}
		if ect != wantECT || recovered != wantRecovered || r.BytesInFlight != flight {
			t.Fatalf("trial %d: ECT %v want %v, recovered %d want %d, flight %d want %d", trial, ect, wantECT, recovered, wantRecovered, r.BytesInFlight, flight)
		}
		if len(lost) == 0 || wantRecovered == 0 {
			t.Fatalf("trial %d exercised no loss or no late ACK", trial)
		}
	}
}

// A pure ACK sent while the ledger is full of packets in flight isn't
// tracked, so no ACK reaches it through recovery; its history record is
// settled when it is sent, and the acknowledged history trims.
func TestUntrackedPureACKHistorySettles(t *testing.T) {
	r, err := NewSent(2)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	for pn := range uint64(2) {
		if err := r.OnSent(Application, Packet{Number: pn, SentAt: now, Bytes: 1200, AckEliciting: true, InFlight: true}); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.OnSent(Application, Packet{Number: 2, SentAt: now, Bytes: 50}); err != nil {
		t.Fatal(err)
	}
	if _, tracked := r.spaces[Application].packets[2]; tracked {
		t.Fatal("fixture must leave the pure ACK untracked")
	}
	if _, err := r.OnACK(Application, ack(0, 2), now.Add(time.Millisecond), 0, 0, true); err != nil {
		t.Fatal(err)
	}
	if n := r.history.order.Len(); n != 0 {
		t.Fatal("acknowledged history not trimmed", n)
	}
}

// A pure ACK evicted from a full ledger to make room for a packet in flight
// is never ACKed through recovery either; its history record is settled at
// eviction, and the acknowledged history trims.
func TestEvictedPureACKHistorySettles(t *testing.T) {
	r, err := NewSent(2)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	for pn := range uint64(2) {
		if err := r.OnSent(Application, Packet{Number: pn, SentAt: now, Bytes: 50}); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.OnSent(Application, Packet{Number: 2, SentAt: now, Bytes: 1200, AckEliciting: true, InFlight: true}); err != nil {
		t.Fatal(err)
	}
	if _, tracked := r.spaces[Application].packets[0]; tracked {
		t.Fatal("fixture must evict the oldest pure ACK")
	}
	if _, err := r.OnACK(Application, ack(1, 2), now.Add(time.Millisecond), 0, 0, true); err != nil {
		t.Fatal(err)
	}
	if n := r.history.order.Len(); n != 0 {
		t.Fatal("acknowledged history not trimmed", n)
	}
}
