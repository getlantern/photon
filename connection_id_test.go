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
		if id.Len() != size || !bytes.Equal(id.Bytes(), original) {
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
	if len(seen) != 21 || (ConnectionID{}) != ConnectionIDFromBytes(nil) || ConnectionIDFromBytes([]byte{1}) == ConnectionIDFromBytes([]byte{1, 0}) {
		t.Fatal("length or zero-value identity")
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
