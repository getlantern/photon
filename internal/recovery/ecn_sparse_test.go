package recovery

import (
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestECNACKSparseAndDenseHistory(t *testing.T) {
	r, _ := NewSent(128)
	at := time.Unix(100, 0)
	marks := make(map[uint64]ECNMark)
	for pn := uint64(0); pn < 100; pn++ {
		mark := ECNMark(pn % 3)
		if err := r.OnSent(Application, Packet{Number: pn, ECN: mark, SentAt: at}); err != nil {
			t.Fatal(err)
		}
		marks[pn] = mark
	}
	frames := []wire.AckFrame{
		{Ranges: []wire.AckRange{{Smallest: 99, Largest: 99}, {Smallest: 50, Largest: 50}, {Smallest: 1, Largest: 1}}},
		ack(2, 40), // late ACKs after packet-threshold loss
		ack(0, 99), // dense range with duplicate and previously lost transmissions
		ack(0, 99),
	}
	var totals [3]uint64
	for _, frame := range frames {
		var want [2]uint64
		for pn, mark := range marks {
			if frame.Acknowledges(pn) {
				switch mark {
				case ECT0:
					want[0]++
				case ECT1:
					want[1]++
				}
				delete(marks, pn)
			}
		}
		totals[0] += want[0]
		totals[1] += want[1]
		frame.ECN = &totals
		got, err := r.OnACK(Application, frame, at.Add(time.Millisecond), 0, 0, true)
		if err != nil || got.NewlyECT != want || got.ECN.Failed {
			t.Fatal(got, want, err)
		}
	}
	if len(r.spaces[Application].ecnPending) != 0 {
		t.Fatal("acknowledged evidence retained")
	}
}

func TestECNACKRangeBoundaryWorkIsBounded(t *testing.T) {
	s := packetSpace{ecnPending: map[uint64]ecnTransmission{
		0: {marking: ECT1}, wire.MaxVarint: {marking: ECT0},
	}}
	s.ecnUntracked.add(0)
	s.ecnUntracked.add(wire.MaxVarint)
	if got := s.ackECN(ack(wire.MaxVarint, wire.MaxVarint), nil); got != [2]uint64{1, 0} {
		t.Fatal("maximum PN lookup", got)
	}
	// This range must find the one remaining entry, never enumerate 2^62 PNs.
	if got := s.ackECN(ack(0, wire.MaxVarint), nil); got != [2]uint64{0, 1} {
		t.Fatal("wide ACK scan", got)
	}
	if len(s.ecnPending) != 0 {
		t.Fatal("evidence retained")
	}
}
