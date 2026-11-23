package transport

import (
	"errors"
	"github.com/getlantern/photon/internal/wire"
	"testing"
)

func TestStreamDeliverySplitAndOverlappingACKs(t *testing.T) {
	original := wire.StreamFrame{StreamID: 4, Offset: 100, Data: make([]byte, 2500), Fin: true, OmitLength: true}
	d, err := newStreamDelivery(original)
	if err != nil {
		t.Fatal(err)
	}
	head, tail, split, err := splitStreamFrame(original, 1140)
	if err != nil || !split || head.Fin || !tail.Fin || head.OmitLength || tail.OmitLength || tail.Offset != 1240 {
		t.Fatal("bad split", err)
	}
	middle, last, split, err := splitStreamFrame(tail, 1140)
	if err != nil || !split || middle.Fin || !last.Fin || last.Offset != 2380 {
		t.Fatal("bad tail split", err)
	}
	calls := 0
	complete := func() error { calls++; return nil }
	for _, f := range []wire.StreamFrame{last, head, head} {
		if done, err := d.acknowledge(f.Offset, len(f.Data), f.Fin, complete); err != nil || done {
			t.Fatal("partial/duplicate completion", err)
		}
	}
	// A late ACK of the original packet covers all children, including bytes
	// already acknowledged on other paths, and invokes completion only once.
	if done, err := d.acknowledge(original.Offset, len(original.Data), original.Fin, complete); err != nil || !done || calls != 1 {
		t.Fatal("original completion", err)
	}
	if done, err := d.acknowledge(middle.Offset, len(middle.Data), false, complete); err != nil || done || calls != 1 {
		t.Fatal("child double completion", err)
	}
}

func TestStreamDeliveryFINAndAtomicCompletion(t *testing.T) {
	d, _ := newStreamDelivery(wire.StreamFrame{Offset: 7, Data: make([]byte, 65), Fin: true})
	calls := 0
	fail := errors.New("completion failed")
	if done, err := d.acknowledge(7, 65, false, func() error { calls++; return nil }); err != nil || done || calls != 0 {
		t.Fatal("bytes completed missing FIN")
	}
	if _, err := d.acknowledge(72, 0, true, func() error { return fail }); !errors.Is(err, fail) || d.finACKed || d.completed {
		t.Fatal("failed completion committed state")
	}
	if done, err := d.acknowledge(72, 0, true, func() error { calls++; return nil }); err != nil || !done || calls != 1 {
		t.Fatal("FIN retry", err)
	}
	empty, _ := newStreamDelivery(wire.StreamFrame{Offset: wire.MaxVarint, Fin: true})
	if done, err := empty.acknowledge(wire.MaxVarint, 0, true, func() error { return nil }); err != nil || !done {
		t.Fatal("empty FIN", err)
	}
}

func TestStreamDeliveryInvalidRanges(t *testing.T) {
	d, _ := newStreamDelivery(wire.StreamFrame{Offset: 100, Data: make([]byte, 128), Fin: true})
	for _, v := range []struct {
		offset uint64
		length int
		fin    bool
	}{{99, 1, false}, {100, -1, false}, {100, 129, false}, {wire.MaxVarint, 1, false}, {100, 1, true}} {
		if _, err := d.acknowledge(v.offset, v.length, v.fin, func() error { t.Fatal("invalid callback"); return nil }); err == nil || d.remaining != 128 || d.finACKed {
			t.Fatal("invalid range changed ledger")
		}
	}
	if _, err := newStreamDelivery(wire.StreamFrame{Offset: wire.MaxVarint, Data: []byte{1}}); err == nil {
		t.Fatal("offset overflow")
	}
	if _, err := newStreamDelivery(wire.StreamFrame{Data: make([]byte, maximumUDPPayload+1)}); err == nil {
		t.Fatal("unbounded ledger")
	}
}

func FuzzStreamDelivery(f *testing.F) {
	f.Add([]byte{64, 0, 63, 1, 64, 64, 1})
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) == 0 {
			return
		}
		size := int(input[0]) + 1
		d, _ := newStreamDelivery(wire.StreamFrame{Offset: 12, Data: make([]byte, size), Fin: true})
		covered := make([]bool, size)
		finSeen := false
		calls := 0
		for i := 1; i+2 < len(input) && i < 301; i += 3 {
			start := int(input[i]) % (size + 1)
			n := int(input[i+1]) % (size - start + 1)
			fin := input[i+2]&1 != 0 && start+n == size
			for j := start; j < start+n; j++ {
				covered[j] = true
			}
			finSeen = finSeen || fin
			_, err := d.acknowledge(uint64(12+start), n, fin, func() error { calls++; return nil })
			if err != nil {
				t.Fatal(err)
			}
			all := finSeen
			for _, v := range covered {
				all = all && v
			}
			if d.completed != all || calls > 1 || (all && calls != 1) {
				t.Fatal("coverage/FIN mismatch")
			}
		}
	})
}
