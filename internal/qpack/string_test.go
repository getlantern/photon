package qpack

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestStringForms(t *testing.T) {
	for prefix := uint8(1); prefix <= 7; prefix++ {
		for _, h := range []bool{false, true} {
			for _, s := range []string{"", "www.example.com", string([]byte{0, 255, 128}), string(bytes.Repeat([]byte("a"), 256))} {
				flags := byte(0)
				if prefix < 7 {
					flags = 0x80
				}
				wire, err := AppendString(nil, flags, prefix, s, h)
				if err != nil {
					t.Fatal(err)
				}
				got, n, err := ConsumeString(append(wire, 0x42), prefix, len(s))
				if err != nil || got != s || n != len(wire) {
					t.Fatalf("%d %t: %q %d %v", prefix, h, got, n, err)
				}
				for cut := 0; cut < len(wire); cut++ {
					_, n, err := ConsumeString(wire[:cut], prefix, len(s))
					if n != 0 || !errors.Is(err, io.ErrUnexpectedEOF) {
						t.Fatalf("cut %d: %d %v", cut, n, err)
					}
				}
				if len(s) > 0 {
					_, n, err := ConsumeString(wire, prefix, len(s)-1)
					if n != 0 || !errors.Is(err, ErrStringLimit) {
						t.Fatal("limit", n, err)
					}
				}
			}
		}
	}
	for _, prefix := range []uint8{0, 8, 255} {
		original := []byte{42}
		got, err := AppendString(original, 0, prefix, "x", false)
		if !errors.Is(err, ErrPrefix) || !bytes.Equal(got, original) {
			t.Fatal("invalid prefix")
		}
	}
	if _, err := AppendString(nil, 0x80, 7, "x", false); !errors.Is(err, ErrPrefix) {
		t.Fatal("overlapping H flag")
	}
	if _, n, err := ConsumeString([]byte{0x81, 0xff}, 7, 20); n != 0 || !errors.Is(err, ErrHuffman) {
		t.Fatal("invalid Huffman", n, err)
	}
}
