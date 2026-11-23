package wire

import (
	"bytes"
	"reflect"
	"testing"
)

func TestAckFrameKnownBytes(t *testing.T) {
	// 98..100, gap 93..97, then 88..92; ECN counts 17, 4, 1.
	b := decodeHex(t, "0340640001020404110401")
	want := AckFrame{Ranges: []AckRange{{98, 100}, {88, 92}}, ECN: &[3]uint64{17, 4, 1}}
	f, n, err := ConsumeAckFrame(append(b, 1))
	if err != nil || n != len(b) || !reflect.DeepEqual(f, want) {
		t.Fatalf("%+v %d %v", f, n, err)
	}
	encoded, err := f.Append(nil)
	if err != nil || !bytes.Equal(encoded, b) {
		t.Fatalf("%x %v", encoded, err)
	}
	for i := 0; i < len(b); i++ {
		if _, _, err := ConsumeAckFrame(b[:i]); err == nil {
			t.Fatalf("truncated ACK at %d", i)
		}
	}
}

func TestAckRangeBoundaries(t *testing.T) {
	for _, ranges := range [][]AckRange{{{0, 0}}, {{0, MaxVarint}}, {{MaxVarint, MaxVarint}}, {{2, 2}, {0, 0}}} {
		f := AckFrame{Delay: MaxVarint, Ranges: ranges}
		b, err := f.Append(nil)
		if err != nil {
			t.Fatal(err)
		}
		got, _, err := ConsumeAckFrame(b)
		if err != nil || !reflect.DeepEqual(f, got) {
			t.Fatal("range boundary", err)
		}
	}
	for _, f := range []AckFrame{
		{}, {Ranges: []AckRange{{1, 0}}}, {Ranges: []AckRange{{0, MaxVarint + 1}}},
		{Ranges: []AckRange{{2, 2}, {1, 1}}}, {Ranges: []AckRange{{2, 4}, {1, 2}}},
		{Delay: MaxVarint + 1, Ranges: []AckRange{{0, 0}}},
		{Ranges: []AckRange{{0, 0}}, ECN: &[3]uint64{MaxVarint + 1, 0, 0}},
	} {
		dst := []byte{0xaa}
		out, err := f.Append(dst)
		if err == nil || !bytes.Equal(out, dst) {
			t.Fatal("accepted invalid ACK or changed dst")
		}
	}
	for _, s := range []string{
		"0200000001",                 // first range underflows
		"02010001000000",             // gap underflows
		"02020001000100",             // gap underflows by one
		"02020001000001",             // second range underflows
		"020000c000000000000000ff00", // advertised count larger than input
		"400200000000",               // nonminimal frame type
	} {
		if _, _, err := ConsumeAckFrame(decodeHex(t, s)); err == nil {
			t.Fatalf("accepted malformed ACK %s", s)
		}
	}
}

func TestStreamFrameFlags(t *testing.T) {
	for typ := byte(8); typ <= 15; typ++ {
		b := []byte{typ, 4}
		offset := uint64(0)
		if typ&4 != 0 {
			offset = 42
			b = append(b, 42)
		}
		if typ&2 != 0 {
			b = append(b, 3)
		}
		b = append(b, 1, 2, 3)
		want := StreamFrame{StreamID: 4, Offset: offset, Fin: typ&1 != 0, OmitLength: typ&2 == 0, Data: []byte{1, 2, 3}}
		f, n, err := ConsumeStreamFrame(b)
		if err != nil || n != len(b) || !reflect.DeepEqual(f, want) {
			t.Fatalf("flags %x: %+v %v", typ, f, err)
		}
		encoded, err := f.Append(nil)
		if err != nil || !bytes.Equal(encoded, b) {
			t.Fatal("stream encode", err)
		}
		withTail := append(bytes.Clone(b), 0xaa)
		got, n, err := ConsumeStreamFrame(withTail)
		if err != nil {
			t.Fatal(err)
		}
		if want.OmitLength {
			if n != len(withTail) || len(got.Data) != 4 {
				t.Fatal("missing implicit remainder")
			}
		} else if n != len(b) || len(got.Data) != 3 {
			t.Fatal("consumed next frame")
		}
	}
}

func TestStreamFrameLimits(t *testing.T) {
	f := StreamFrame{StreamID: MaxVarint, Offset: MaxVarint, Fin: true}
	b, err := f.Append(nil)
	if err != nil {
		t.Fatal(err)
	}
	got, n, err := ConsumeStreamFrame(b)
	if err != nil || n != len(b) || got.Offset != MaxVarint || !got.Fin || len(got.Data) != 0 {
		t.Fatal("empty FIN at limit", err)
	}
	f.Data = []byte{1}
	if _, err = f.Append(nil); err == nil {
		t.Fatal("offset overflow")
	}
	for _, bad := range [][]byte{nil, {8}, {0x0c, 0}, {0x0a, 0}, {0x0a, 0, 4, 1}, {0x40, 8, 0}} {
		if _, _, err := ConsumeStreamFrame(bad); err == nil {
			t.Fatal("invalid stream", bad)
		}
	}
	bad := []byte{0x0e, 0}
	bad, _ = AppendVarint(bad, MaxVarint)
	bad = append(bad, 1, 1)
	if _, _, err := ConsumeStreamFrame(bad); err == nil {
		t.Fatal("decoded offset overflow")
	}
}

func FuzzAckFrame(f *testing.F) {
	f.Add([]byte{2, 0, 0, 0, 0})
	f.Add(decodeHex(f, "0340640001020404110401"))
	f.Fuzz(func(t *testing.T, b []byte) {
		frame, n, err := ConsumeAckFrame(b)
		if err != nil {
			return
		}
		if n > len(b) || n < 5 {
			t.Fatal("bounds")
		}
		encoded, err := frame.Append(nil)
		if err != nil {
			t.Fatal(err)
		}
		again, used, err := ConsumeAckFrame(encoded)
		if err != nil || used != len(encoded) || !reflect.DeepEqual(frame, again) {
			t.Fatal("ACK round trip", err)
		}
	})
}

func FuzzStreamFrame(f *testing.F) {
	f.Add([]byte{8, 0})
	f.Add([]byte{15, 1, 2, 3, 4, 5, 6})
	f.Fuzz(func(t *testing.T, b []byte) {
		frame, n, err := ConsumeStreamFrame(b)
		if err != nil {
			return
		}
		if n > len(b) || n < 2 {
			t.Fatal("bounds")
		}
		encoded, err := frame.Append(nil)
		if err != nil {
			t.Fatal(err)
		}
		again, used, err := ConsumeStreamFrame(encoded)
		if err != nil || used != len(encoded) || !reflect.DeepEqual(frame, again) {
			t.Fatal("STREAM round trip", err)
		}
	})
}
