package quic

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestConnectionIDValueOwnership(t *testing.T) {
	seen := make(map[ConnectionID]int)
	for size := 0; size <= 20; size++ {
		input := make([]byte, size)
		for i := range input {
			input[i] = byte(i*17 + size)
		}
		original := bytes.Clone(input)
		id := ConnectionIDFromBytes(input)
		seen[id] = size
		if !id.Valid() || id.Len() != size || !bytes.Equal(id.Bytes(), original) {
			t.Fatal("round trip", size)
		}
		if size > 0 {
			input[0] ^= 0xff
			view := id.Bytes()
			view[0] ^= 0xff
		}
		if !bytes.Equal(id.Bytes(), original) || seen[id] != size {
			t.Fatal("mutable value", size)
		}
		want := hex.EncodeToString(original)
		if size == 0 {
			want = "(empty)"
		}
		if id.String() != want {
			t.Fatal(id.String(), want)
		}
		if id != ConnectionIDFromBytes(original) {
			t.Fatal("equal bytes produced unequal values")
		}
	}
	if len(seen) != 21 || (ConnectionID{}) == ConnectionIDFromBytes(nil) || ConnectionIDFromBytes([]byte{1}) == ConnectionIDFromBytes([]byte{1, 0}) {
		t.Fatal("length or zero-value identity")
	}
}

func TestConnectionIDValid(t *testing.T) {
	var zero ConnectionID
	if zero.Valid() || zero.Len() != 0 || len(zero.Bytes()) != 0 || zero.String() != "(invalid)" {
		t.Fatal("zero value", zero.Valid(), zero.Len(), zero.Bytes(), zero.String())
	}
	for size := 0; size <= 20; size++ {
		if id := ConnectionIDFromBytes(make([]byte, size)); !id.Valid() || id == zero {
			t.Fatal("constructed ID invalid", size)
		}
	}
	if _, ok := map[ConnectionID]bool{ConnectionIDFromBytes([]byte{}): true}[zero]; ok {
		t.Fatal("zero value collides with the zero-length ID")
	}
}

func TestConnectionIDRejectsOversize(t *testing.T) {
	for _, size := range []int{21, 255, 1024} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("accepted %d bytes", size)
				}
			}()
			ConnectionIDFromBytes(make([]byte, size))
		}()
	}
}
