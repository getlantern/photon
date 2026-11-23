package qpack

import (
	"reflect"
	"testing"
)

func TestInsertFieldPolicy(t *testing.T) {
	e, _ := NewEncoderTable(256, 1, 8, 16)
	peer, _ := NewDecoderTable(256)
	capWire, err := e.Apply(EncoderInstruction{Type: SetDynamicCapacity, Index: 256})
	if err != nil {
		t.Fatal(err)
	}
	peer.ConsumeEncoderUpdate(capWire, 256)
	for _, f := range []DecodedField{{Field: Field{":method", "GET"}}, {Field: Field{"secret", "value"}, NeverIndex: true}} {
		wire, inserted, err := e.InsertField(f, 256)
		if err != nil || inserted || wire != nil || e.InsertCount() != 0 {
			t.Fatal(wire, inserted, err)
		}
	}
	cases := []struct {
		f      Field
		typ    EncoderInstructionType
		static bool
	}{
		{Field{"custom-key", "one"}, InsertLiteralName, false},
		{Field{"custom-key", "two"}, InsertNameReference, false},
		{Field{":path", "/new"}, InsertNameReference, true},
	}
	for _, c := range cases {
		wire, inserted, err := e.InsertField(DecodedField{Field: c.f}, 256)
		if err != nil || !inserted {
			t.Fatal(inserted, err)
		}
		instruction, _, err := ConsumeEncoderInstruction(wire, 256)
		if err != nil || instruction.Type != c.typ || instruction.Static != c.static {
			t.Fatal(instruction, err)
		}
		if _, err := peer.ConsumeEncoderUpdate(wire, 256); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(snapshotEncoderTable(e.table), snapshotEncoderTable(peer)) {
			t.Fatal("wire divergence")
		}
		if wire, inserted, err := e.InsertField(DecodedField{Field: c.f}, 256); err != nil || inserted || wire != nil {
			t.Fatal("duplicate", inserted, err)
		}
	}
}
func TestInsertFieldBudgetAndProtection(t *testing.T) {
	e, _ := NewEncoderTable(34, 1, 8, 16)
	e.Apply(EncoderInstruction{Type: SetDynamicCapacity, Index: 34})
	field := DecodedField{Field: Field{"a", "b"}}
	for _, budget := range []int{0, 1, 2, 3} {
		if wire, inserted, err := e.InsertField(field, budget); err != nil || inserted || wire != nil || e.InsertCount() != 0 {
			t.Fatal(budget, inserted, err)
		}
	}
	wire, inserted, err := e.InsertField(field, 4)
	if err != nil || !inserted || len(wire) != 4 {
		t.Fatal(wire, inserted, err)
	}
	before := snapshotEncoderTable(e.table)
	next := DecodedField{Field: Field{"a", "c"}}
	if wire, inserted, err := e.InsertField(next, 64); err != nil || inserted || wire != nil || !reflect.DeepEqual(before, snapshotEncoderTable(e.table)) {
		t.Fatal("unreceived eviction", err)
	}
	e.ApplyFeedback(DecoderInstruction{Type: InsertCountIncrement, Value: 1})
	e.Track(4, []uint64{0})
	if _, inserted, err := e.InsertField(next, 64); err != nil || inserted {
		t.Fatal("protected eviction", err)
	}
	e.ApplyFeedback(DecoderInstruction{Type: SectionAcknowledgment, Value: 4})
	if _, inserted, err := e.InsertField(next, 64); err != nil || !inserted {
		t.Fatal(inserted, err)
	}
}
func TestInsertSelectDecodeCycle(t *testing.T) {
	e, _ := NewEncoderTable(128, 1, 8, 16)
	d := decoderForTest(t)
	capWire, _ := e.Apply(EncoderInstruction{Type: SetDynamicCapacity, Index: 128})
	d.ConsumeEncoder(capWire)
	fields := []DecodedField{{Field: Field{"custom-key", "custom-value"}}, {Field: Field{"secret", "private"}, NeverIndex: true}}
	update, inserted, err := e.InsertField(fields[0], 128)
	if err != nil || !inserted {
		t.Fatal(err)
	}
	section, err := e.EncodeFields(4, fields, 256, 256, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, blocked, err := d.Submit(4, section); err != nil || !blocked {
		t.Fatal(blocked, err)
	}
	_, ready, feedback, err := d.ConsumeEncoder(update)
	if err != nil || len(ready) != 1 || !reflect.DeepEqual(ready[0].Fields, fields) {
		t.Fatal(ready, err)
	}
	for _, instruction := range feedback {
		if err := e.ApplyFeedback(instruction); err != nil {
			t.Fatal(err)
		}
	}
	if e.KnownReceivedCount() != 1 || e.refs.sections != 0 {
		t.Fatal("feedback")
	}
}
func FuzzInsertField(f *testing.F) {
	f.Add("custom-key", "custom-value", false, uint16(128))
	f.Fuzz(func(t *testing.T, name, value string, sensitive bool, budget uint16) {
		if len(name)+len(value) > 4096 {
			t.Skip()
		}
		e, _ := NewEncoderTable(8192, 1, 8, 16)
		peer, _ := NewDecoderTable(8192)
		wire, _ := e.Apply(EncoderInstruction{Type: SetDynamicCapacity, Index: 8192})
		peer.ConsumeEncoderUpdate(wire, 8192)
		update, inserted, err := e.InsertField(DecodedField{Field: Field{name, value}, NeverIndex: sensitive}, int(budget))
		if err != nil {
			t.Fatal(err)
		}
		if !inserted {
			if update != nil || e.InsertCount() != 0 {
				t.Fatal("skipped mutation")
			}
			return
		}
		if sensitive || len(update) > int(budget) {
			t.Fatal("policy")
		}
		if n, err := peer.ConsumeEncoderUpdate(update, 8192); err != nil || n != len(update) {
			t.Fatal(n, err)
		}
		if !reflect.DeepEqual(snapshotEncoderTable(e.table), snapshotEncoderTable(peer)) {
			t.Fatal("table divergence")
		}
	})
}
