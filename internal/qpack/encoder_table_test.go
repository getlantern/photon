package qpack

import (
	"errors"
	"reflect"
	"testing"
)

func TestEncoderTableAtomicEviction(t *testing.T) {
	e, _ := NewEncoderTable(68, 2, 8, 16)
	apply := func(v EncoderInstruction) {
		t.Helper()
		if _, err := e.Apply(v); err != nil {
			t.Fatal(err)
		}
	}
	apply(EncoderInstruction{Type: SetDynamicCapacity, Index: 68})
	apply(EncoderInstruction{Type: InsertLiteralName, Name: "a", Value: "b"})
	apply(EncoderInstruction{Type: InsertLiteralName, Name: "c", Value: "d"})
	e.ApplyFeedback(DecoderInstruction{Type: InsertCountIncrement, Value: 2})
	e.Track(4, []uint64{1})
	before := snapshotEncoderTable(e.table)
	// Entry zero is evictable but entry one is protected: neither may be removed.
	for _, v := range []EncoderInstruction{{Type: SetDynamicCapacity}, {Type: InsertLiteralName, Name: "longer", Value: "value"}} {
		wire, err := e.Apply(v)
		if !errors.Is(err, ErrEntryProtected) || wire != nil || !reflect.DeepEqual(before, snapshotEncoderTable(e.table)) || e.refs.dropped != 0 {
			t.Fatal("partial eviction", err)
		}
	}
	e.ApplyFeedback(DecoderInstruction{Type: SectionAcknowledgment, Value: 4})
	apply(EncoderInstruction{Type: SetDynamicCapacity})
	if e.Size() != 0 || e.InsertCount() != 2 || e.refs.dropped != 2 {
		t.Fatal("clearing")
	}
	apply(EncoderInstruction{Type: SetDynamicCapacity, Index: 68})
	apply(EncoderInstruction{Type: InsertLiteralName, Name: "x", Value: "y"})
	if _, err := e.Absolute(2); err != nil {
		t.Fatal(err)
	}
}
func TestEncoderTableSelfReferenceAndWire(t *testing.T) {
	e, _ := NewEncoderTable(34, 1, 8, 16)
	peer, _ := NewDecoderTable(34)
	apply := func(v EncoderInstruction) {
		t.Helper()
		wire, err := e.Apply(v)
		if err != nil {
			t.Fatal(err)
		}
		n, err := peer.ConsumeEncoderUpdate(wire, 64)
		if err != nil || n != len(wire) {
			t.Fatal(n, err)
		}
		if !reflect.DeepEqual(snapshotEncoderTable(peer), snapshotEncoderTable(e.table)) {
			t.Fatal("peer diverged")
		}
	}
	apply(EncoderInstruction{Type: SetDynamicCapacity, Index: 34})
	apply(EncoderInstruction{Type: InsertLiteralName, Name: "a", Value: "b", HuffmanName: true, HuffmanValue: true})
	if _, err := e.Apply(EncoderInstruction{Type: DuplicateEntry}); !errors.Is(err, ErrEntryProtected) {
		t.Fatal("unreceived victim", err)
	}
	e.ApplyFeedback(DecoderInstruction{Type: InsertCountIncrement, Value: 1})
	apply(EncoderInstruction{Type: DuplicateEntry})
	e.ApplyFeedback(DecoderInstruction{Type: InsertCountIncrement, Value: 1})
	apply(EncoderInstruction{Type: InsertNameReference, Value: "c", HuffmanValue: true})
	got, _ := e.Absolute(2)
	if got != (Field{"a", "c"}) {
		t.Fatal(got)
	}
	before := snapshotEncoderTable(e.table)
	for _, v := range []EncoderInstruction{{Type: SetDynamicCapacity, Index: 35}, {Type: InsertNameReference, Static: true, Index: 99}, {Type: DuplicateEntry, Index: 1}, {Type: InsertLiteralName, Name: "oversized"}, {Type: 255}} {
		if wire, err := e.Apply(v); err == nil || wire != nil || !reflect.DeepEqual(before, snapshotEncoderTable(e.table)) {
			t.Fatal("invalid mutation", v, err)
		}
	}
}
func FuzzEncoderTable(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8})
	f.Fuzz(func(t *testing.T, data []byte) {
		e, _ := NewEncoderTable(128, 2, 8, 16)
		peer, _ := NewDecoderTable(128)
		for _, op := range data {
			var v EncoderInstruction
			switch op % 6 {
			case 0:
				v = EncoderInstruction{Type: SetDynamicCapacity, Index: uint64(op)}
			case 1:
				v = EncoderInstruction{Type: InsertLiteralName, Name: "a", Value: string([]byte{op})}
			case 2:
				v = EncoderInstruction{Type: DuplicateEntry, Index: uint64(op % 4)}
			case 3:
				if e.InsertCount() > 0 {
					e.Track(uint64(op%4)*4, []uint64{e.InsertCount() - 1})
				}
				continue
			case 4:
				e.ApplyFeedback(DecoderInstruction{Type: StreamCancellation, Value: uint64(op%4) * 4})
				continue
			case 5:
				e.ApplyFeedback(DecoderInstruction{Type: InsertCountIncrement, Value: 1})
				continue
			}
			before := snapshotEncoderTable(e.table)
			dropped, inserted := e.refs.dropped, e.refs.inserted
			wire, err := e.Apply(v)
			if err != nil {
				if wire != nil || !reflect.DeepEqual(before, snapshotEncoderTable(e.table)) || dropped != e.refs.dropped || inserted != e.refs.inserted {
					t.Fatal("non-atomic error")
				}
				continue
			}
			if n, err := peer.ConsumeEncoderUpdate(wire, 128); err != nil || n != len(wire) {
				t.Fatal("wire", err)
			}
			if !reflect.DeepEqual(snapshotEncoderTable(peer), snapshotEncoderTable(e.table)) {
				t.Fatal("table divergence")
			}
			if e.refs.inserted != e.table.inserts || e.refs.dropped != e.table.inserts-uint64(e.table.Len()) {
				t.Fatal("reference divergence")
			}
		}
	})
}
