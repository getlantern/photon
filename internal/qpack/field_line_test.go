package qpack

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestFieldLineForms(t *testing.T) {
	cases := []struct {
		v    FieldLine
		wire []byte
	}{
		{FieldLine{Type: IndexedField, Static: true, Index: 17}, []byte{0xd1}},
		{FieldLine{Type: IndexedField, Index: 1}, []byte{0x81}},
		{FieldLine{Type: IndexedPostBase, Index: 1}, []byte{0x11}},
		{FieldLine{Type: LiteralNameReference, Static: true, NeverIndex: true, Index: 1, Value: "x"}, []byte{0x71, 1, 'x'}},
		{FieldLine{Type: LiteralNameReference, Index: 1, Value: "x"}, []byte{0x41, 1, 'x'}},
		{FieldLine{Type: LiteralPostBase, NeverIndex: true, Index: 1, Value: "x"}, []byte{9, 1, 'x'}},
		{FieldLine{Type: LiteralName, NeverIndex: true, Name: "a", Value: "b"}, []byte{0x31, 'a', 1, 'b'}},
	}
	for _, c := range cases {
		encoded, err := AppendFieldLine(nil, c.v)
		if err != nil || !bytes.Equal(encoded, c.wire) {
			t.Fatalf("encode %+v: %x %v", c.v, encoded, err)
		}
		got, n, err := ConsumeFieldLine(append(bytes.Clone(c.wire), 0xff), 32)
		if err != nil || got != c.v || n != len(c.wire) {
			t.Fatalf("decode %+v: %+v %d %v", c.v, got, n, err)
		}
		for i := 0; i < len(c.wire); i++ {
			_, n, err := ConsumeFieldLine(c.wire[:i], 32)
			if n != 0 || !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("cut %d: %d %v", i, n, err)
			}
		}
	}
	for _, hn := range []bool{false, true} {
		for _, hv := range []bool{false, true} {
			v := FieldLine{Type: LiteralName, Name: "custom-key", Value: "custom-value", NeverIndex: true, HuffmanName: hn, HuffmanValue: hv}
			wire, err := AppendFieldLine(nil, v)
			if err != nil {
				t.Fatal(err)
			}
			got, n, err := ConsumeFieldLine(wire, 22)
			if err != nil || got != v || n != len(wire) {
				t.Fatal("Huffman roundtrip", got, n, err)
			}
			if _, n, err := ConsumeFieldLine(wire, 21); n != 0 || !errors.Is(err, ErrStringLimit) {
				t.Fatal("limit", n, err)
			}
		}
	}
	for _, v := range []FieldLine{{Type: 255}, {Type: IndexedField, NeverIndex: true}, {Type: IndexedPostBase, Static: true}, {Type: LiteralPostBase, Static: true}, {Type: IndexedField, Index: MaxInteger + 1}} {
		dst := []byte{42}
		got, err := AppendFieldLine(dst, v)
		if err == nil || !bytes.Equal(dst, got) {
			t.Fatal("invalid encoding accepted", v)
		}
	}
}
func TestResolveFieldLine(t *testing.T) {
	table, _ := NewDecoderTable(128)
	table.SetCapacity(128)
	table.Insert(Field{"a", "one"})
	table.Insert(Field{"b", "two"})
	for _, c := range []struct {
		v                   FieldLine
		base, required, ref uint64
		want                Field
	}{
		{FieldLine{Type: IndexedField, Index: 1}, 2, 2, 1, Field{"a", "one"}},
		{FieldLine{Type: IndexedPostBase, Index: 1}, 0, 2, 2, Field{"b", "two"}},
		{FieldLine{Type: LiteralPostBase, Value: "new"}, 1, 2, 2, Field{"b", "new"}},
		{FieldLine{Type: LiteralNameReference, Value: "new"}, 1, 2, 1, Field{"a", "new"}},
		{FieldLine{Type: IndexedField, Static: true, Index: 17}, 0, 0, 0, Field{":method", "GET"}},
		{FieldLine{Type: LiteralNameReference, Static: true, Value: "host"}, 0, 0, 0, Field{":authority", "host"}},
		{FieldLine{Type: LiteralName, Name: "a", Value: "b"}, 0, 0, 0, Field{"a", "b"}},
	} {
		got, ref, err := table.ResolveFieldLine(c.v, c.base, c.required)
		if err != nil || got != c.want || ref != c.ref {
			t.Fatalf("%+v: %+v %d %v", c, got, ref, err)
		}
	}
	for _, c := range []struct {
		v              FieldLine
		base, required uint64
	}{
		{FieldLine{Type: IndexedField}, 0, 2},
		{FieldLine{Type: IndexedField, Index: 2}, 2, 2},
		{FieldLine{Type: IndexedPostBase, Index: 1}, 1, 2},
		{FieldLine{Type: IndexedPostBase, Index: 1}, MaxInteger, MaxInteger},
		{FieldLine{Type: IndexedPostBase}, 2, 3},
		{FieldLine{Type: IndexedField, Static: true, Index: 99}, 0, 0},
	} {
		if _, _, err := table.ResolveFieldLine(c.v, c.base, c.required); err == nil {
			t.Fatalf("invalid reference %+v", c)
		}
	}
	table.SetCapacity(36) // Evict a, retaining b.
	if _, _, err := table.ResolveFieldLine(FieldLine{Type: IndexedField, Index: 1}, 2, 2); err == nil {
		t.Fatal("evicted reference accepted")
	}
}
func FuzzFieldLine(f *testing.F) {
	f.Add([]byte{0xd1})
	f.Add([]byte{0x31, 'a', 1, 'b'})
	f.Add([]byte{9, 1, 'x'})
	f.Fuzz(func(t *testing.T, data []byte) {
		v, n, err := ConsumeFieldLine(data, 4096)
		if err != nil {
			if n != 0 {
				t.Fatal("consumed on failure")
			}
			return
		}
		encoded, err := AppendFieldLine(nil, v)
		if err != nil {
			t.Fatal(err)
		}
		got, m, err := ConsumeFieldLine(encoded, 4096)
		if err != nil || got != v || m != len(encoded) {
			t.Fatal("roundtrip")
		}
		if n <= 0 || n > len(data) {
			t.Fatal("invalid consumption")
		}
	})
}
