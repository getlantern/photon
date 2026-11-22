package qpack

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestEncodeSectionDynamicRFC(t *testing.T) {
	e, _ := NewEncoderTable(220, 1, 8, 16)
	var d *Decoder
	// Use a matching 220-byte advertised maximum for the RFC modulo encoding.
	d, _ = NewDecoder(DecoderLimits{MaxTableCapacity: 220, MaxBlockedStreams: 1, MaxBufferedBytes: 128, MaxEncodedSection: 128, MaxDecodedSection: 1024, MaxFields: 16, MaxInstructionLiteralBytes: 128})
	var updates [][]byte
	for _, v := range []EncoderInstruction{{Type: SetDynamicCapacity, Index: 220}, {Type: InsertNameReference, Static: true, Index: 0, Value: "www.example.com"}, {Type: InsertNameReference, Static: true, Index: 1, Value: "/sample/path"}} {
		wire, err := e.Apply(v)
		if err != nil {
			t.Fatal(err)
		}
		updates = append(updates, wire)
	}
	lines := []FieldLine{{Type: IndexedPostBase}, {Type: IndexedPostBase, Index: 1}}
	wire, err := e.EncodeSection(4, 0, lines, 4, 106, 2)
	if err != nil || !bytes.Equal(wire, []byte{3, 0x81, 0x10, 0x11}) {
		t.Fatal(wire, err)
	}
	if _, _, blocked, err := d.Submit(4, wire); !blocked || err != nil {
		t.Fatal(blocked, err)
	}
	for _, update := range updates {
		_, ready, feedback, err := d.ConsumeEncoder(update)
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range feedback {
			if err := e.ApplyFeedback(v); err != nil {
				t.Fatal(err)
			}
		}
		if len(ready) > 0 && (len(ready) != 1 || len(ready[0].Fields) != 2) {
			t.Fatal(ready)
		}
	}
	if e.refs.sections != 0 || e.KnownReceivedCount() != 2 {
		t.Fatal("feedback state")
	}
}
func TestEncodeSectionAtomicLimits(t *testing.T) {
	e, _ := NewEncoderTable(64, 0, 8, 16)
	e.Apply(EncoderInstruction{Type: SetDynamicCapacity, Index: 64})
	e.Apply(EncoderInstruction{Type: InsertLiteralName, Name: "a", Value: "b"})
	lines := []FieldLine{{Type: IndexedField}}
	before := snapshotEncoderTable(e.table)
	if wire, err := e.EncodeSection(4, 1, lines, 64, 64, 1); !errors.Is(err, ErrBlockedStreams) || wire != nil || e.refs.sections != 0 {
		t.Fatal(wire, err)
	}
	e.ApplyFeedback(DecoderInstruction{Type: InsertCountIncrement, Value: 1})
	for _, limits := range []struct {
		encoded int
		decoded uint64
		fields  int
	}{{2, 34, 1}, {3, 33, 1}, {3, 34, 0}} {
		if wire, err := e.EncodeSection(4, 1, lines, limits.encoded, limits.decoded, limits.fields); !errors.Is(err, ErrSectionLimit) || wire != nil || e.refs.sections != 0 {
			t.Fatal(wire, err)
		}
	}
	if _, err := e.EncodeSection(4, 1, []FieldLine{{Type: IndexedField, Index: 1}}, 64, 64, 1); !errors.Is(err, ErrTableIndex) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, snapshotEncoderTable(e.table)) {
		t.Fatal("table changed")
	}
	wire, err := e.EncodeSection(4, 1, lines, 3, 34, 1)
	if err != nil || len(wire) != 3 || e.refs.sections != 1 {
		t.Fatal(wire, err)
	}
}
func FuzzEncodeSection(f *testing.F) {
	f.Add("custom-key", "custom-value", true)
	f.Fuzz(func(t *testing.T, name, value string, h bool) {
		if len(name)+len(value) > 32768 {
			t.Skip()
		}
		e, _ := NewEncoderTable(0, 0, 0, 0)
		lines := []FieldLine{{Type: IndexedField, Static: true, Index: 17}, {Type: LiteralName, Name: name, Value: value, NeverIndex: true, HuffmanName: h, HuffmanValue: h}}
		wire, err := e.EncodeSection(4, 0, lines, 262144, 65536, 2)
		if err != nil {
			t.Fatal(err)
		}
		_, got, err := e.table.DecodeSection(wire, 65536, 2)
		if err != nil || len(got) != 2 || got[1].Name != name || got[1].Value != value || !got[1].NeverIndex {
			t.Fatal(got, err)
		}
		if _, err := e.EncodeSection(4, 0, lines, len(wire)-1, 65536, 2); !errors.Is(err, ErrSectionLimit) {
			t.Fatal("encoded limit", err)
		}
	})
}
