package qpack

import "testing"

func TestDecoderTableEviction(t *testing.T) {
	table, _ := NewDecoderTable(100)
	if _, err := table.Insert(Field{"a", "b"}); err != ErrTableCapacity {
		t.Fatal(err)
	}
	if err := table.SetCapacity(100); err != nil {
		t.Fatal(err)
	}
	for i, f := range []Field{{"a", "b"}, {"c", "d"}, {"e", "f"}} {
		id, err := table.Insert(f)
		if err != nil || id != uint64(i) {
			t.Fatal(id, err)
		}
	}
	if table.Len() != 2 || table.Size() != 68 || table.InsertCount() != 3 {
		t.Fatal("wrong table state")
	}
	if _, err := table.Absolute(0); err != ErrTableIndex {
		t.Fatal(err)
	}
	if f, err := table.Relative(0); err != nil || f != (Field{"e", "f"}) {
		t.Fatal(f, err)
	}
	// Duplicate oldest live entry, which must be evicted to fit its replacement.
	if id, err := table.Duplicate(1); err != nil || id != 3 {
		t.Fatal(id, err)
	}
	if f, err := table.Absolute(3); err != nil || f != (Field{"c", "d"}) {
		t.Fatal(f, err)
	}
	if _, err := table.Absolute(1); err != ErrTableIndex {
		t.Fatal(err)
	}
	if err := table.SetCapacity(0); err != nil {
		t.Fatal(err)
	}
	if table.Len() != 0 || table.Size() != 0 || table.InsertCount() != 4 || table.entries != nil {
		t.Fatal("clear lost count or retained entries")
	}
	table.SetCapacity(32)
	if id, err := table.Insert(Field{}); err != nil || id != 4 {
		t.Fatal(id, err)
	}
}
func TestDecoderTableRejectedUpdates(t *testing.T) {
	table, _ := NewDecoderTable(64)
	table.SetCapacity(64)
	table.Insert(Field{"x", "y"})
	if err := table.SetCapacity(65); err != ErrTableCapacity {
		t.Fatal(err)
	}
	if _, err := table.Insert(Field{"name", "abcdefghijklmnopqrstuvwxyz0123456789"}); err != ErrTableCapacity {
		t.Fatal(err)
	}
	if _, err := table.Duplicate(MaxInteger); err != ErrTableIndex {
		t.Fatal(err)
	}
	if table.Size() != 34 || table.Capacity() != 64 || table.InsertCount() != 1 {
		t.Fatal("failed operation mutated table")
	}
	table.inserts = MaxInteger
	if _, err := table.Insert(Field{}); err != ErrIntegerOverflow {
		t.Fatal(err)
	}
	if table.Size() != 34 || table.InsertCount() != MaxInteger {
		t.Fatal("overflow evicted entries")
	}
}
func TestDecoderTableBoundedHistory(t *testing.T) {
	table, _ := NewDecoderTable(128)
	table.SetCapacity(128)
	for i := uint64(0); i < 10000; i++ {
		id, err := table.Insert(Field{})
		if err != nil || id != i {
			t.Fatal(id, err)
		}
		if table.Len() > 4 || len(table.entries) > 8 {
			t.Fatal("storage grows with insert count")
		}
		for j := uint64(0); j < uint64(table.Len()); j++ {
			if _, err := table.Absolute(i - j); err != nil {
				t.Fatal(err)
			}
		}
	}
}
func FuzzDecoderTable(f *testing.F) {
	f.Add([]byte{128, 1, 2, 3, 0, 64, 9, 255})
	f.Fuzz(func(t *testing.T, ops []byte) {
		table, _ := NewDecoderTable(255)
		var model []Field
		var count, capacity uint64
		for i, op := range ops {
			if i%3 == 0 {
				capacity = uint64(op)
				table.SetCapacity(capacity)
			} else {
				field := Field{string([]byte{op}), string([]byte{op, op})}
				if capacity >= 35 {
					if _, err := table.Insert(field); err != nil {
						t.Fatal(err)
					}
					model = append(model, field)
					count++
				} else if _, err := table.Insert(field); err != ErrTableCapacity {
					t.Fatal(err)
				}
			}
			for uint64(len(model))*35 > capacity {
				model = model[1:]
			}
			if table.Len() != len(model) || table.Size() != uint64(len(model))*35 || table.InsertCount() != count {
				t.Fatal("model mismatch")
			}
			for j, want := range model {
				got, err := table.Absolute(count - uint64(len(model)) + uint64(j))
				if err != nil || got != want {
					t.Fatal("lookup mismatch")
				}
			}
		}
	})
}
