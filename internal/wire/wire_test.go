package wire

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"testing"
)

func TestVarintVectors(t *testing.T) {
	for _, tc := range []struct {
		value uint64
		hex   string
	}{
		{0, "00"}, {63, "3f"}, {64, "4040"}, {16383, "7fff"}, {16384, "80004000"},
		{1<<30 - 1, "bfffffff"}, {1 << 30, "c000000040000000"}, {MaxVarint, "ffffffffffffffff"},
		{37, "25"}, {15293, "7bbd"}, {494878333, "9d7f3e7d"}, {151288809941952652, "c2197c5eff14e88c"},
	} {
		b, err := AppendVarint(nil, tc.value)
		if err != nil || hex.EncodeToString(b) != tc.hex {
			t.Fatalf("encode %d: %x, %v", tc.value, b, err)
		}
		v, n, err := ConsumeVarint(b)
		if err != nil || v != tc.value || n != len(b) {
			t.Fatalf("decode %x: %d %d %v", b, v, n, err)
		}
		for i := 0; i < len(b); i++ {
			_, n, err := ConsumeVarint(b[:i])
			if !errors.Is(err, io.ErrUnexpectedEOF) || n != 0 {
				t.Fatal("accepted truncated integer")
			}
		}
	}
	for _, b := range [][]byte{{0x40, 37}, {0x80, 0, 0, 37}, {0xc0, 0, 0, 0, 0, 0, 0, 37}} {
		v, n, err := ConsumeVarint(b)
		if err != nil || v != 37 || n != len(b) {
			t.Fatal("rejected legal non-minimal encoding")
		}
	}
	dst := []byte{0xaa}
	b, err := AppendVarint(dst, MaxVarint+1)
	if !errors.Is(err, ErrVarintRange) || !bytes.Equal(b, dst) {
		t.Fatal("out-of-range integer")
	}
}

func TestPacketNumber(t *testing.T) {
	for _, tc := range []struct {
		truncated      uint64
		n              int
		expected, want uint64
	}{
		{0, 1, 0, 0}, {0x9b32, 2, 0xa82f31, 0xa89b32},
		{0, 1, 255, 256}, {255, 1, 256, 255}, {0, 1, 128, 256}, {128, 1, 0, 128},
		{0, 1, 129, 256}, {255, 1, 128, 255}, {255, 1, 127, 255},
		{255, 1, MaxVarint, MaxVarint}, {255, 1, MaxVarint + 1, MaxVarint},
	} {
		got, err := DecodePacketNumber(tc.truncated, tc.n, tc.expected)
		if err != nil || got != tc.want {
			t.Fatalf("%+v: %d %v", tc, got, err)
		}
	}
	for _, tc := range []struct {
		v uint64
		n int
		e uint64
	}{{0, 0, 0}, {0, 5, 0}, {256, 1, 0}, {0, 1, MaxVarint + 2}, {0, 1, MaxVarint + 1}} {
		if _, err := DecodePacketNumber(tc.v, tc.n, tc.e); err == nil {
			t.Fatalf("accepted invalid packet number %+v", tc)
		}
	}
}

func TestLongHeader(t *testing.T) {
	for _, v := range []Version{Version1, Version2} {
		for _, typ := range []PacketType{Initial, Handshake, ZeroRTT} {
			var token []byte
			if typ == Initial {
				token = []byte("token")
			}
			b, err := AppendLongHeader(nil, v, typ, []byte("destination"), []byte("source"), token, 0x12345, 3, 20)
			if err != nil {
				t.Fatal(err)
			}
			hlen := len(b)
			b = append(b, make([]byte, 27)...)
			h, err := ParseHeader(b, 0)
			if err != nil || h.Version != v || h.Type != typ || h.PNOffset != hlen-3 || h.PacketLen != hlen+20 || !bytes.Equal(h.Token, token) {
				t.Fatalf("%+v %v", h, err)
			}
			for i := 0; i < h.PacketLen; i++ {
				if _, err := ParseHeader(b[:i], 0); err == nil {
					t.Fatalf("accepted truncated long packet: %d", i)
				}
			}
		}
	}
}

func TestVersionTypeMapping(t *testing.T) {
	for _, tc := range []struct {
		v     Version
		bytes []byte
	}{{Version1, []byte{0xc0, 0xd0, 0xe0, 0xf0}}, {Version2, []byte{0xd0, 0xe0, 0xf0, 0xc0}}} {
		for i, b := range tc.bytes {
			bits, err := tc.v.TypeBits(PacketType(i))
			if err != nil || 0xc0|bits<<4 != b {
				t.Fatal("type encoding")
			}
			typ, err := tc.v.PacketType(b)
			if err != nil || typ != PacketType(i) {
				t.Fatal("type decoding")
			}
		}
	}
}

func TestInvariantHeaders(t *testing.T) {
	// Version negotiation ignores the fixed bit and permits longer invariant CIDs.
	b := []byte{0x80, 0, 0, 0, 0, 21}
	b = append(b, make([]byte, 21)...)
	b = append(b, 0, 0, 0, 0, 1, 0x6b, 0x33, 0x43, 0xcf)
	h, err := ParseHeader(b, 0)
	if err != nil || len(h.Versions) != 2 || h.Versions[1] != Version2 {
		t.Fatalf("%+v %v", h, err)
	}
	for _, bad := range [][]byte{b[:len(b)-1], {0x80, 0, 0, 0, 0, 0, 0}, {0xc0, 0, 0, 0, 1, 21}, {0x40}} {
		if _, err := ParseHeader(bad, 8); err == nil {
			t.Fatalf("accepted malformed header %x", bad)
		}
	}
	b[4] = 10
	h, err = ParseHeader(b, 0)
	if !errors.Is(err, ErrVersion) || len(h.DCID) != 21 {
		t.Fatal("unknown version invariants")
	}
	h, err = ParseHeader([]byte{0x40, 1, 2, 3}, 2)
	if err != nil || h.PNOffset != 3 || h.PacketLen != 4 {
		t.Fatal("short header")
	}
}

func FuzzVarint(f *testing.F) {
	for _, v := range []uint64{0, 63, 64, 16384, MaxVarint, MaxVarint + 1} {
		f.Add(v)
	}
	f.Fuzz(func(t *testing.T, v uint64) {
		b, err := AppendVarint(nil, v)
		if v > MaxVarint {
			if err == nil {
				t.Fatal("overflow")
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		x, n, err := ConsumeVarint(b)
		if err != nil || x != v || n != VarintLen(v) {
			t.Fatal("round trip")
		}
	})
}

func FuzzHeader(f *testing.F) {
	f.Add([]byte{0xc0, 0, 0, 0, 1, 0, 0, 0, 20}, uint8(0))
	f.Add([]byte{0x40}, uint8(8))
	f.Fuzz(func(t *testing.T, b []byte, cid uint8) {
		h, err := ParseHeader(b, int(cid))
		if err != nil {
			return
		}
		if h.PacketLen > len(b) || h.PNOffset < 0 || h.PNOffset > h.PacketLen {
			t.Fatal("invalid parser bounds")
		}
	})
}

func BenchmarkVarint(b *testing.B) {
	var dst [8]byte
	for b.Loop() {
		out, _ := AppendVarint(dst[:0], 151288809941952652)
		_, _, _ = ConsumeVarint(out)
	}
}
