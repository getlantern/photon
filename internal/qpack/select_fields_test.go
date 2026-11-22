package qpack

import (
	"reflect"
	"testing"
)

func TestSelectFieldsSensitiveAndDynamic(t *testing.T) {
	e, _ := NewEncoderTable(256, 1, 8, 16)
	e.Apply(EncoderInstruction{Type: SetDynamicCapacity, Index: 256})
	e.Apply(EncoderInstruction{Type: InsertLiteralName, Name: "custom-key", Value: "custom-value"})
	fields := []DecodedField{{Field: Field{":method", "GET"}}, {Field: Field{"custom-key", "custom-value"}}, {Field: Field{"custom-key", "custom-value"}, NeverIndex: true}, {Field: Field{":method", "GET"}, NeverIndex: true}}
	wire, err := e.EncodeFields(4, fields, 256, 512, 4)
	if err != nil {
		t.Fatal(err)
	}
	p, n, err := ConsumeSectionPrefix(wire, 256, 1)
	if err != nil || p.RequiredInsertCount != 1 {
		t.Fatal(p, err)
	}
	for i, typ := range []FieldLineType{IndexedField, IndexedField, LiteralNameReference, LiteralNameReference} {
		line, m, err := ConsumeFieldLine(wire[n:], 512)
		if err != nil || line.Type != typ || line.NeverIndex != fields[i].NeverIndex {
			t.Fatal(i, line, err)
		}
		n += m
	}
	_, decoded, err := e.table.DecodeSection(wire, 512, 4)
	if err != nil || !reflect.DeepEqual(decoded, fields) {
		t.Fatal(decoded, err)
	}
	// Stream 4 occupies the sole blocking slot; stream 8 must fall back.
	wire, err = e.EncodeFields(8, fields, 256, 512, 4)
	if err != nil {
		t.Fatal(err)
	}
	p, _, err = ConsumeSectionPrefix(wire, 256, 1)
	if err != nil || p.RequiredInsertCount != 0 {
		t.Fatal("blocking fallback", p, err)
	}
}
func TestSelectFieldsTrackingFallback(t *testing.T) {
	for _, limits := range [][2]int{{0, 8}, {8, 0}, {8, 1}} {
		e, _ := NewEncoderTable(256, 1, limits[0], limits[1])
		e.Apply(EncoderInstruction{Type: SetDynamicCapacity, Index: 256})
		e.Apply(EncoderInstruction{Type: InsertLiteralName, Name: "long-name", Value: "long-value"})
		fields := []DecodedField{{Field: Field{"long-name", "long-value"}}, {Field: Field{"long-name", "long-value"}}}
		wire, err := e.EncodeFields(4, fields, 256, 256, 2)
		if err != nil {
			t.Fatal(limits, err)
		}
		_, got, err := e.table.DecodeSection(wire, 256, 2)
		if err != nil || !reflect.DeepEqual(got, fields) {
			t.Fatal(got, err)
		}
		if e.refs.refs > limits[1] || e.refs.sections > limits[0] {
			t.Fatal("tracking budget")
		}
	}
}
func FuzzSelectFields(f *testing.F) {
	f.Add("custom-key", "custom-value", true)
	f.Fuzz(func(t *testing.T, name, value string, sensitive bool) {
		if len(name)+len(value) > 4096 {
			t.Skip()
		}
		e, _ := NewEncoderTable(8192, 1, 4, 4)
		e.Apply(EncoderInstruction{Type: SetDynamicCapacity, Index: 8192})
		e.Apply(EncoderInstruction{Type: InsertLiteralName, Name: name, Value: value})
		fields := []DecodedField{{Field: Field{name, value}, NeverIndex: sensitive}, {Field: Field{":path", "/"}}}
		wire, err := e.EncodeFields(4, fields, 32768, 8192, 2)
		if err != nil {
			t.Fatal(err)
		}
		_, got, err := e.table.DecodeSection(wire, 8192, 2)
		if err != nil || !reflect.DeepEqual(got, fields) {
			t.Fatal(got, err)
		}
	})
}
