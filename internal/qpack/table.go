package qpack

import (
	"errors"
	"strings"
)

var (
	ErrTableCapacity = errors.New("qpack: dynamic table capacity exceeded")
	ErrTableIndex    = errors.New("qpack: unavailable dynamic table index")
)

type Field struct{ Name, Value string }

func (f Field) size() (uint64, error) {
	name, value := uint64(len(f.Name)), uint64(len(f.Value))
	if name > MaxInteger-32 || value > MaxInteger-32-name {
		return 0, ErrIntegerOverflow
	}
	return name + value + 32, nil
}

// DecoderTable processes the peer encoder's table updates. It is serialized by
// the future decoder owner, not safe for concurrent use. An encoder needs a
// separate outstanding-reference policy before using these eviction operations.
// Storage grows with retained entries, never with absolute insertion indices.
type DecoderTable struct {
	maximum, capacity, size, inserts uint64
	entries                          []Field
	head                             int
}

func NewDecoderTable(maximum uint64) (*DecoderTable, error) {
	if maximum > MaxInteger {
		return nil, ErrIntegerOverflow
	}
	return &DecoderTable{maximum: maximum}, nil
}
func (t *DecoderTable) Capacity() uint64    { return t.capacity }
func (t *DecoderTable) Size() uint64        { return t.size }
func (t *DecoderTable) InsertCount() uint64 { return t.inserts }
func (t *DecoderTable) Len() int            { return len(t.entries) - t.head }
func (t *DecoderTable) SetCapacity(capacity uint64) error {
	if capacity > t.maximum {
		return ErrTableCapacity
	}
	t.evictTo(capacity)
	t.capacity = capacity
	return nil
}
func (t *DecoderTable) evictTo(limit uint64) {
	for t.size > limit {
		size, _ := t.entries[t.head].size()
		t.size -= size
		t.entries[t.head] = Field{} // Release evicted strings immediately.
		t.head++
	}
	if t.head == len(t.entries) {
		t.entries = nil
		t.head = 0
	}
}
func (t *DecoderTable) Insert(f Field) (uint64, error) {
	size, err := f.size()
	if err != nil {
		return 0, err
	}
	if size > t.capacity {
		return 0, ErrTableCapacity
	}
	if t.inserts == MaxInteger {
		return 0, ErrIntegerOverflow
	}
	// Own only the field bytes, not an arbitrarily large source string backing
	// allocation. Clone before eviction so duplicate/self-reference is safe.
	owned := Field{strings.Clone(f.Name), strings.Clone(f.Value)}
	t.evictTo(t.capacity - size)
	if t.head > 0 && t.head >= t.Len() {
		live := copy(t.entries, t.entries[t.head:])
		clear(t.entries[live:])
		t.entries = t.entries[:live]
		t.head = 0
	}
	id := t.inserts
	t.entries = append(t.entries, owned)
	t.size += size
	t.inserts++
	return id, nil
}
func (t *DecoderTable) Absolute(id uint64) (Field, error) {
	if id >= t.inserts {
		return Field{}, ErrTableIndex
	}
	first := t.inserts - uint64(t.Len())
	if id < first {
		return Field{}, ErrTableIndex
	}
	return t.entries[t.head+int(id-first)], nil
}

// Relative uses the encoder instruction stream's current insertion count.
func (t *DecoderTable) Relative(index uint64) (Field, error) {
	if index >= t.inserts {
		return Field{}, ErrTableIndex
	}
	return t.Absolute(t.inserts - index - 1)
}
func (t *DecoderTable) Duplicate(index uint64) (uint64, error) {
	f, err := t.Relative(index)
	if err != nil {
		return 0, err
	}
	return t.Insert(f)
}
