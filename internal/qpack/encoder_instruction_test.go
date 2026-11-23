package qpack

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"reflect"
	"testing"
)

func TestEncoderInstructionsRFC(t *testing.T) {
	table, _ := NewDecoderTable(220)
	cases := []struct {
		wire        string
		instruction EncoderInstruction
		count, size uint64
	}{
		{"3fbd01", EncoderInstruction{Type: SetDynamicCapacity, Index: 220}, 0, 0},
		{"c00f7777772e6578616d706c652e636f6d", EncoderInstruction{Type: InsertNameReference, Static: true, Index: 0, Value: "www.example.com"}, 1, 57},
		{"c10c2f73616d706c652f70617468", EncoderInstruction{Type: InsertNameReference, Static: true, Index: 1, Value: "/sample/path"}, 2, 106},
		{"4a637573746f6d2d6b65790c637573746f6d2d76616c7565", EncoderInstruction{Type: InsertLiteralName, Name: "custom-key", Value: "custom-value"}, 3, 160},
		{"02", EncoderInstruction{Type: DuplicateEntry, Index: 2}, 4, 217},
	}
	for _, c := range cases {
		wire, err := hex.DecodeString(c.wire)
		if err != nil {
			t.Fatal(err)
		}
		got, n, err := ConsumeEncoderInstruction(append(bytes.Clone(wire), 0x20), 128)
		if err != nil || n != len(wire) || got != c.instruction {
			t.Fatalf("%x: %+v %d %v", wire, got, n, err)
		}
		encoded, err := AppendEncoderInstruction(nil, c.instruction)
		if err != nil || !bytes.Equal(encoded, wire) {
			t.Fatalf("encode %x %v", encoded, err)
		}
		for cut := 0; cut < len(wire); cut++ {
			old := snapshotEncoderTable(table)
			n, err := table.ConsumeEncoderUpdate(wire[:cut], 128)
			if n != 0 || !errors.Is(err, io.ErrUnexpectedEOF) || !reflect.DeepEqual(old, snapshotEncoderTable(table)) {
				t.Fatalf("cut %d: %d %v", cut, n, err)
			}
		}
		n, err = table.ConsumeEncoderUpdate(wire, 128)
		if err != nil || n != len(wire) || table.InsertCount() != c.count || table.Size() != c.size {
			t.Fatalf("apply %x: %d %v %+v", wire, n, err, table)
		}
	}
}

type encoderSnapshot struct {
	Capacity, Size, Count uint64
	Fields                []Field
}

func snapshotEncoderTable(table *DecoderTable) encoderSnapshot {
	s := encoderSnapshot{Capacity: table.Capacity(), Size: table.Size(), Count: table.InsertCount()}
	for id := table.InsertCount() - uint64(table.Len()); id < table.InsertCount(); id++ {
		f, _ := table.Absolute(id)
		s.Fields = append(s.Fields, f)
	}
	return s
}
func TestEncoderUpdateFailuresAndSelfEviction(t *testing.T) {
	table, _ := NewDecoderTable(40)
	table.SetCapacity(40)
	table.Insert(Field{"key", "old"})
	for _, v := range []EncoderInstruction{
		{Type: SetDynamicCapacity, Index: 41},
		{Type: DuplicateEntry, Index: 1},
		{Type: InsertNameReference, Static: true, Index: 99, Value: "x"},
		{Type: InsertNameReference, Index: 1, Value: "x"},
		{Type: InsertLiteralName, Name: "too-long-name", Value: "x"},
	} {
		old := snapshotEncoderTable(table)
		wire, err := AppendEncoderInstruction(nil, v)
		if err != nil {
			t.Fatal(err)
		}
		n, err := table.ConsumeEncoderUpdate(wire, 128)
		if err == nil || n != 0 || !reflect.DeepEqual(old, snapshotEncoderTable(table)) {
			t.Fatalf("failed update changed state: %+v %v", v, err)
		}
	}
	v := EncoderInstruction{Type: InsertNameReference, Value: "new"}
	if err := table.ApplyEncoderInstruction(v); err != nil {
		t.Fatal(err)
	}
	got, err := table.Relative(0)
	if err != nil || got != (Field{"key", "new"}) || table.Len() != 1 || table.InsertCount() != 2 {
		t.Fatalf("self eviction %+v %v", got, err)
	}
	if _, err := table.Absolute(0); !errors.Is(err, ErrTableIndex) {
		t.Fatal("old entry retained")
	}
}
func TestEncoderInstructionHuffmanAndLimits(t *testing.T) {
	for _, hn := range []bool{false, true} {
		for _, hv := range []bool{false, true} {
			v := EncoderInstruction{Type: InsertLiteralName, Name: "custom-key", Value: "custom-value", HuffmanName: hn, HuffmanValue: hv}
			wire, err := AppendEncoderInstruction(nil, v)
			if err != nil {
				t.Fatal(err)
			}
			got, n, err := ConsumeEncoderInstruction(wire, 22)
			if err != nil || got != v || n != len(wire) {
				t.Fatalf("%+v %d %v", got, n, err)
			}
			if _, n, err := ConsumeEncoderInstruction(wire, 21); n != 0 || !errors.Is(err, ErrStringLimit) {
				t.Fatal("combined limit", n, err)
			}
		}
	}
	original := []byte{42}
	for _, v := range []EncoderInstruction{{Type: 255}, {Type: SetDynamicCapacity, Index: MaxInteger + 1}} {
		got, err := AppendEncoderInstruction(original, v)
		if err == nil || !bytes.Equal(got, original) {
			t.Fatal("invalid encode")
		}
	}
}
func FuzzEncoderInstructions(f *testing.F) {
	f.Add([]byte{0x3f, 0xbd, 1, 0xc0, 1, 'x'})
	f.Add([]byte{0x41, 'a', 1, 'b', 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		table, _ := NewDecoderTable(256)
		for len(data) > 0 {
			old := snapshotEncoderTable(table)
			v, n, err := ConsumeEncoderInstruction(data, 256)
			applied, applyErr := table.ConsumeEncoderUpdate(data, 256)
			if applyErr != nil && (applied != 0 || !reflect.DeepEqual(old, snapshotEncoderTable(table))) {
				t.Fatal("non-atomic failure")
			}
			if err != nil {
				if n != 0 || applyErr == nil {
					t.Fatal("parse failure")
				}
				break
			}
			wire, err := AppendEncoderInstruction(nil, v)
			if err != nil {
				t.Fatal(err)
			}
			got, m, err := ConsumeEncoderInstruction(wire, 256)
			if err != nil || got != v || m != len(wire) {
				t.Fatal("round trip")
			}
			if n <= 0 || n > len(data) {
				t.Fatal("progress")
			}
			data = data[n:]
		}
	})
}
