package qpack

import (
	"errors"
	"testing"
)

func TestEncoderReferencesFIFO(t *testing.T) {
	e, _ := NewEncoderReferences(1, 8, 16)
	for i := 0; i < 3; i++ {
		e.RecordInsertion()
	}
	refs := []uint64{0, 0}
	if ric, err := e.Track(4, refs); ric != 1 || err != nil {
		t.Fatal(ric, err)
	}
	refs[0] = 2
	if _, err := e.Track(4, []uint64{2}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Track(8, []uint64{1}); !errors.Is(err, ErrBlockedStreams) {
		t.Fatal(err)
	}
	if e.BlockedStreams() != 1 || e.CanEvict(0) {
		t.Fatal("protection")
	}
	if err := e.ApplyFeedback(DecoderInstruction{Type: SectionAcknowledgment, Value: 4}); err != nil {
		t.Fatal(err)
	}
	if e.known != 1 || !e.CanEvict(0) || e.BlockedStreams() != 1 || e.sections != 1 {
		t.Fatal("FIFO ACK", e)
	}
	if err := e.RecordEviction(0); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Track(8, []uint64{0}); !errors.Is(err, ErrTableIndex) {
		t.Fatal("evicted ref", err)
	}
	if err := e.ApplyFeedback(DecoderInstruction{Type: InsertCountIncrement, Value: 2}); err != nil {
		t.Fatal(err)
	}
	if e.BlockedStreams() != 0 || e.CanEvict(2) {
		t.Fatal("increment released reference")
	}
	if _, err := e.Track(8, []uint64{1}); err != nil {
		t.Fatal(err)
	}
	e.ApplyFeedback(DecoderInstruction{Type: StreamCancellation, Value: 4})
	if e.known != 3 || !e.CanEvict(2) || e.CanEvict(1) {
		t.Fatal("cancellation")
	}
	e.ApplyFeedback(DecoderInstruction{Type: StreamCancellation, Value: 8})
	if e.sections != 0 || e.refs != 0 || len(e.streams) != 0 || len(e.protection) != 0 {
		t.Fatal("retained tracking")
	}
}
func TestEncoderReferencesFeedbackAndLimits(t *testing.T) {
	e, _ := NewEncoderReferences(0, 1, 1)
	e.RecordInsertion()
	if _, err := e.Track(4, []uint64{0}); !errors.Is(err, ErrBlockedStreams) {
		t.Fatal(err)
	}
	for _, v := range []DecoderInstruction{{Type: InsertCountIncrement}, {Type: InsertCountIncrement, Value: 2}, {Type: SectionAcknowledgment, Value: 4}, {Type: 255}, {Type: StreamCancellation, Value: MaxInteger + 1}} {
		if err := e.ApplyFeedback(v); !errors.Is(err, ErrDecoderFeedback) || e.known != 0 || e.sections != 0 {
			t.Fatal(v, err)
		}
	}
	e.ApplyFeedback(DecoderInstruction{Type: StreamCancellation, Value: 4})
	if e.known != 0 || e.CanEvict(0) {
		t.Fatal("cancellation implied receipt")
	}
	e.ApplyFeedback(DecoderInstruction{Type: InsertCountIncrement, Value: 1})
	if _, err := e.Track(4, []uint64{0}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Track(8, []uint64{0}); !errors.Is(err, ErrEncoderLimit) {
		t.Fatal(err)
	}
	if _, err := e.Track(8, nil); err != nil || e.sections != 1 {
		t.Fatal("static section tracked", err)
	}
	if err := e.RecordEviction(0); !errors.Is(err, ErrEntryProtected) {
		t.Fatal(err)
	}
	e.ApplyFeedback(DecoderInstruction{Type: SectionAcknowledgment, Value: 4})
	if err := e.ApplyFeedback(DecoderInstruction{Type: SectionAcknowledgment, Value: 4}); !errors.Is(err, ErrDecoderFeedback) {
		t.Fatal("duplicate ACK", err)
	}
}
func FuzzEncoderReferences(f *testing.F) {
	f.Add([]byte{0, 0, 1, 2, 3, 4, 5, 6, 7})
	f.Fuzz(func(t *testing.T, data []byte) {
		e, _ := NewEncoderReferences(2, 8, 16)
		for _, op := range data {
			id := uint64(op%4) * 4
			switch op % 6 {
			case 0:
				e.RecordInsertion()
			case 1:
				if e.inserted > 0 {
					e.Track(id, []uint64{e.inserted - 1})
				}
			case 2:
				e.ApplyFeedback(DecoderInstruction{Type: SectionAcknowledgment, Value: id})
			case 3:
				e.ApplyFeedback(DecoderInstruction{Type: StreamCancellation, Value: id})
			case 4:
				e.ApplyFeedback(DecoderInstruction{Type: InsertCountIncrement, Value: 1})
			case 5:
				e.RecordEviction(e.dropped)
			}
			if e.known > e.inserted || e.dropped > e.known || e.BlockedStreams() > 2 || e.sections > 8 || e.refs > 16 {
				t.Fatal("bounds")
			}
			counts := map[uint64]int{}
			sections, refs := 0, 0
			for _, q := range e.streams {
				for _, s := range q {
					sections++
					refs += len(s.refs)
					for _, ref := range s.refs {
						if ref < e.dropped || ref >= e.inserted {
							t.Fatal("dead ref")
						}
						counts[ref]++
					}
				}
			}
			if sections != e.sections || refs != e.refs || len(counts) != len(e.protection) {
				t.Fatal("accounting")
			}
			for id, count := range counts {
				if e.protection[id] != count {
					t.Fatal("reference count")
				}
			}
		}
	})
}

func TestEncoderReferencesWithDecoderFeedback(t *testing.T) {
	e, _ := NewEncoderReferences(1, 8, 16)
	d := decoderForTest(t)
	feedDecoder(t, d, EncoderInstruction{Type: SetDynamicCapacity, Index: 128})
	e.RecordInsertion()
	required, err := e.Track(4, []uint64{0})
	if err != nil {
		t.Fatal(err)
	}
	wire, _ := AppendSectionPrefix(nil, SectionPrefix{RequiredInsertCount: required, Base: 1}, 128)
	wire = append(wire, 0x80)
	if _, _, blocked, err := d.Submit(4, wire); err != nil || !blocked {
		t.Fatal(blocked, err)
	}
	ready, feedback := feedDecoder(t, d, EncoderInstruction{Type: InsertLiteralName, Name: "a", Value: "b"})
	if len(ready) != 1 {
		t.Fatal(ready)
	}
	for _, v := range feedback {
		if err := e.ApplyFeedback(v); err != nil {
			t.Fatal(err)
		}
	}
	if e.KnownReceivedCount() != 1 || e.OutstandingSections() != 0 || !e.CanEvict(0) {
		t.Fatal("ack did not release reference")
	}
	e.RecordInsertion()
	_, feedback = feedDecoder(t, d, EncoderInstruction{Type: DuplicateEntry})
	for _, v := range feedback {
		if err := e.ApplyFeedback(v); err != nil {
			t.Fatal(err)
		}
	}
	if e.KnownReceivedCount() != 2 {
		t.Fatal("speculative insertion")
	}
}
