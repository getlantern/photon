package interop

import (
	"bytes"
	local "github.com/getlantern/photon/internal/qpack"
	"golang.org/x/net/http2/hpack"
	"testing"
)

func TestHuffmanIndependentCodec(t *testing.T) {
	check := func(raw []byte) {
		t.Helper()
		ours, ourErr := local.DecodeHuffman(raw, 65536)
		theirs, theirErr := hpack.HuffmanDecodeToString(raw)
		if (ourErr == nil) != (theirErr == nil) || (ourErr == nil && ours != theirs) {
			t.Fatalf("decode %x: ours %q %v; peer %q %v", raw, ours, ourErr, theirs, theirErr)
		}
	}
	// Exhaust every one- and two-octet wire input, covering padding and invalid
	// tails independently of the implementation's own encoder.
	for a := 0; a < 256; a++ {
		check([]byte{byte(a)})
		for b := 0; b < 256; b++ {
			check([]byte{byte(a), byte(b)})
		}
	}
	var data []byte
	for a := 0; a < 256; a++ {
		data = append(data, byte(a))
	}
	for _, s := range []string{string(data), "www.example.com", "", string(bytes.Repeat(data, 16))} {
		ours := local.AppendHuffman(nil, s)
		theirs := hpack.AppendHuffmanString(nil, s)
		if !bytes.Equal(ours, theirs) {
			t.Fatal("encoded bytes differ")
		}
		check(ours)
	}
}
func FuzzHuffmanIndependentCodec(f *testing.F) {
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})
	f.Add([]byte("test"))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 32768 {
			t.Skip()
		}
		ours, ourErr := local.DecodeHuffman(raw, 65536)
		theirs, theirErr := hpack.HuffmanDecodeToString(raw)
		if (ourErr == nil) != (theirErr == nil) || (ourErr == nil && ours != theirs) {
			t.Fatalf("decode %x: ours %q %v; peer %q %v", raw, ours, ourErr, theirs, theirErr)
		}
		if !bytes.Equal(local.AppendHuffman(nil, string(raw)), hpack.AppendHuffmanString(nil, string(raw))) {
			t.Fatal("encode mismatch")
		}
	})
}
