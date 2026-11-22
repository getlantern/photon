package qpack

import (
	"errors"
	"reflect"
	"testing"
)

func TestSectionDynamicRFC(t *testing.T) {
	table, _ := NewDecoderTable(220)
	table.SetCapacity(220)
	wire := []byte{3, 0x81, 0x10, 0x11} // Appendix B.2: RIC=2, Base=0, post-base 0/1.
	p, fields, err := table.DecodeSection(wire, 106, 2)
	if !errors.Is(err, ErrSectionBlocked) || fields != nil || p != (SectionPrefix{2, 0}) {
		t.Fatal(p, fields, err)
	}
	table.Insert(Field{":authority", "www.example.com"})
	if _, err := table.DecodeSectionFields(wire[2:], p, 106, 2); !errors.Is(err, ErrSectionBlocked) {
		t.Fatal(err)
	}
	table.Insert(Field{":path", "/sample/path"})
	fields, err = table.DecodeSectionFields(wire[2:], p, 106, 2)
	want := []DecodedField{{Field: Field{":authority", "www.example.com"}}, {Field: Field{":path", "/sample/path"}}}
	if err != nil || !reflect.DeepEqual(fields, want) {
		t.Fatal(fields, err)
	}
	for _, limits := range []struct {
		size  uint64
		count int
	}{{105, 2}, {106, 1}, {0, 0}} {
		_, fields, err := table.DecodeSection(wire, limits.size, limits.count)
		if !errors.Is(err, ErrSectionLimit) || fields != nil {
			t.Fatal("limit", fields, err)
		}
	}
	// Nonzero RIC must exactly equal greatest referenced absolute index + 1.
	for _, bad := range [][]byte{{3, 0x81}, {3, 0x81, 0x10}, {3, 0x81, 0xd1}, {2, 0, 0x10}} {
		_, fields, err := table.DecodeSection(bad, 1024, 10)
		if err == nil || fields != nil {
			t.Fatalf("invalid count %x: %v", bad, err)
		}
	}
	table.SetCapacity(0)
	if _, _, err := table.DecodeSection(wire, 1024, 10); !errors.Is(err, ErrTableIndex) {
		t.Fatal("evicted", err)
	}
}
func TestSectionOrderingAndLimits(t *testing.T) {
	table, _ := NewDecoderTable(0)
	wire := []byte{0, 0, 0xd1, 0xd1}
	wire, _ = AppendFieldLine(wire, FieldLine{Type: LiteralName, Name: "x", Value: "y", NeverIndex: true, HuffmanName: true, HuffmanValue: true})
	want := []DecodedField{{Field: Field{":method", "GET"}}, {Field: Field{":method", "GET"}}, {Field: Field{"x", "y"}, NeverIndex: true}}
	_, got, err := table.DecodeSection(wire, 118, 3)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal(got, err)
	}
	for _, bad := range [][]byte{nil, {0}, {0, 0, 0xff}, {0, 0, 0xd1, 0x21}} {
		_, got, err := table.DecodeSection(bad, 1024, 32)
		if err == nil || got != nil {
			t.Fatal("invalid", bad, got, err)
		}
	}
	if _, got, err := table.DecodeSection([]byte{0, 0}, 0, 0); err != nil || len(got) != 0 {
		t.Fatal("empty", got, err)
	}
	if _, _, err := table.DecodeSection(wire, 117, 3); !errors.Is(err, ErrSectionLimit) {
		t.Fatal("indexed accounting", err)
	}
}
func TestSectionSavedPrefixAcrossWrap(t *testing.T) {
	table, _ := NewDecoderTable(64)
	table.SetCapacity(64)
	p, _, err := table.DecodeSection([]byte{2, 0, 0x80}, 1024, 10)
	if !errors.Is(err, ErrSectionBlocked) || p.RequiredInsertCount != 1 {
		t.Fatal(p, err)
	}
	for i := 0; i < 5; i++ {
		table.Insert(Field{"a", "b"})
	}
	// Reconstructing the prefix now would alias count 1 to count 5. The saved
	// prefix must instead reject the original referenced entry as evicted.
	if _, err := table.DecodeSectionFields([]byte{0x80}, p, 1024, 10); !errors.Is(err, ErrTableIndex) {
		t.Fatal("wrapped stale reference", err)
	}
}
func FuzzSection(f *testing.F) {
	f.Add([]byte{0, 0, 0xd1})
	f.Add([]byte{3, 0x81, 0x10, 0x11})
	f.Fuzz(func(t *testing.T, wire []byte) {
		table, _ := NewDecoderTable(128)
		table.SetCapacity(128)
		table.Insert(Field{"a", "b"})
		table.Insert(Field{"c", "d"})
		before := snapshotEncoderTable(table)
		_, fields, err := table.DecodeSection(wire, 4096, 64)
		if !reflect.DeepEqual(before, snapshotEncoderTable(table)) {
			t.Fatal("table mutation")
		}
		if err != nil {
			if fields != nil {
				t.Fatal("partial fields")
			}
			return
		}
		var size uint64
		for _, v := range fields {
			n, _ := v.Field.size()
			size += n
		}
		if size > 4096 || len(fields) > 64 {
			t.Fatal("limit")
		}
	})
}
