package qpack

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"
)

func TestHuffmanRFCVectors(t *testing.T) {
	for _, v := range []struct{ plain, encoded string }{
		{"www.example.com", "f1e3c2e5f23a6ba0ab90f4ff"},
		{"no-cache", "a8eb10649cbf"},
		{"custom-key", "25a849e95ba97d7f"},
		{"custom-value", "25a849e95bb8e8b4bf"},
		{"", ""},
	} {
		want, err := hex.DecodeString(v.encoded)
		if err != nil {
			t.Fatal(err)
		}
		got := AppendHuffman(nil, v.plain)
		if !bytes.Equal(got, want) || HuffmanSize(v.plain) != uint64(len(want)) {
			t.Fatalf("encode %q: %x", v.plain, got)
		}
		plain, err := DecodeHuffman(want, len(v.plain))
		if err != nil || plain != v.plain {
			t.Fatalf("decode %x: %q %v", want, plain, err)
		}
	}
}
func TestHuffmanLimitsAndPadding(t *testing.T) {
	for _, bad := range [][]byte{{0xff}, {0xff, 0xff}, {0xff, 0xff, 0xff, 0xff}, {0x18}, {0x00}} {
		if _, err := DecodeHuffman(bad, 100); !errors.Is(err, ErrHuffman) {
			t.Fatalf("%x: %v", bad, err)
		}
	}
	for _, limit := range []int{-1, 0, 2} {
		if _, err := DecodeHuffman(AppendHuffman(nil, "abc"), limit); !errors.Is(err, ErrStringLimit) {
			t.Fatalf("limit %d: %v", limit, err)
		}
	}
	// Include every octet, including invalid UTF-8; QPACK strings are opaque bytes.
	var all []byte
	for i := 0; i < 256; i++ {
		all = append(all, byte(i))
		s := string([]byte{byte(i)})
		encoded := AppendHuffman(nil, s)
		got, err := DecodeHuffman(encoded, 1)
		if got != s || err != nil {
			t.Fatalf("symbol %d: %x %v", i, encoded, err)
		}
	}
	encoded := AppendHuffman(nil, string(all))
	got, err := DecodeHuffman(encoded, len(all))
	if got != string(all) || err != nil || HuffmanSize(string(all)) != uint64(len(encoded)) {
		t.Fatal("all octets", err)
	}
}
func FuzzHuffman(f *testing.F) {
	f.Add([]byte("www.example.com"))
	f.Add([]byte{0xff, 0, 0xc3, 0xa9})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		encoded := AppendHuffman(nil, string(data))
		if HuffmanSize(string(data)) != uint64(len(encoded)) {
			t.Fatal("size")
		}
		decoded, err := DecodeHuffman(encoded, len(data))
		if err != nil || decoded != string(data) {
			t.Fatal("round trip", err)
		}
		decoded, err = DecodeHuffman(data, 4096)
		if err == nil && !bytes.Equal(AppendHuffman(nil, decoded), data) {
			t.Fatal("noncanonical valid string")
		}
	})
}
