package qpack

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestIntegerVectors(t *testing.T) {
	// RFC 7541 Appendix C.1 vectors, inherited by QPACK.
	for _, tc := range []struct {
		value  uint64
		prefix uint8
		wire   []byte
	}{
		{10, 5, []byte{0x0a}}, {1337, 5, []byte{0x1f, 0x9a, 0x0a}}, {42, 8, []byte{0x2a}},
	} {
		got, err := AppendInteger(nil, 0, tc.prefix, tc.value)
		if err != nil || !bytes.Equal(got, tc.wire) {
			t.Fatalf("%x %v", got, err)
		}
		value, n, err := ConsumeInteger(tc.wire, tc.prefix)
		if err != nil || n != len(tc.wire) || value != tc.value {
			t.Fatalf("%d %d %v", value, n, err)
		}
	}
}
func TestIntegerBoundaries(t *testing.T) {
	for prefix := uint8(1); prefix <= 8; prefix++ {
		mask := uint64(1<<prefix) - 1
		flags := byte(^byte(mask))
		for _, value := range []uint64{0, mask - 1, mask, mask + 1, 127, 128, 16384, 1 << 32, MaxInteger} {
			b, err := AppendInteger(nil, flags, prefix, value)
			if err != nil {
				t.Fatal(err)
			}
			if b[0]&flags != flags {
				t.Fatal("opcode bits lost")
			}
			got, n, err := ConsumeInteger(append(b, 99), prefix)
			if err != nil || got != value || n != len(b) {
				t.Fatalf("prefix=%d value=%d got=%d n=%d error=%v", prefix, value, got, n, err)
			}
			for i := 0; i < len(b); i++ {
				_, n, err := ConsumeInteger(b[:i], prefix)
				if !errors.Is(err, io.ErrUnexpectedEOF) || n != 0 {
					t.Fatalf("partial %x: %d %v", b[:i], n, err)
				}
			}
		}
	}
	// Redundant zero continuation groups are accepted, but bounded.
	padded := append([]byte{31}, bytes.Repeat([]byte{128}, 14)...)
	padded = append(padded, 0)
	if v, n, err := ConsumeInteger(padded, 5); err != nil || v != 31 || n != 16 {
		t.Fatalf("%d %d %v", v, n, err)
	}
	padded[15] = 128
	if _, n, err := ConsumeInteger(append(padded, 0), 5); err != ErrIntegerLength || n != 0 {
		t.Fatalf("%d %v", n, err)
	}
	overflow := append([]byte{31}, bytes.Repeat([]byte{255}, 8)...)
	overflow = append(overflow, 127)
	if _, n, err := ConsumeInteger(overflow, 5); err != ErrIntegerOverflow || n != 0 {
		t.Fatalf("%d %v", n, err)
	}
	for _, tc := range []struct {
		flags  byte
		prefix uint8
		value  uint64
	}{{0, 0, 1}, {0, 9, 1}, {1, 5, 1}, {0, 8, MaxInteger + 1}} {
		dst := []byte{99}
		got, err := AppendInteger(dst, tc.flags, tc.prefix, tc.value)
		if err == nil || !bytes.Equal(dst, got) {
			t.Fatalf("%x %v", got, err)
		}
	}
}
func TestDecoderInstructions(t *testing.T) {
	for _, tc := range []struct {
		instruction DecoderInstruction
		wire        []byte
	}{
		{DecoderInstruction{SectionAcknowledgment, 0}, []byte{128}},
		{DecoderInstruction{SectionAcknowledgment, 127}, []byte{255, 0}},
		{DecoderInstruction{StreamCancellation, 0}, []byte{64}},
		{DecoderInstruction{StreamCancellation, 63}, []byte{127, 0}},
		{DecoderInstruction{InsertCountIncrement, 1}, []byte{1}},
		{DecoderInstruction{InsertCountIncrement, 63}, []byte{63, 0}},
	} {
		b, err := AppendDecoderInstruction(nil, tc.instruction)
		if err != nil || !bytes.Equal(b, tc.wire) {
			t.Fatalf("%x %v", b, err)
		}
		got, n, err := ConsumeDecoderInstruction(append(b, 0))
		if err != nil || got != tc.instruction || n != len(b) {
			t.Fatalf("%v %d %v", got, n, err)
		}
	}
	if _, n, err := ConsumeDecoderInstruction([]byte{0}); err != ErrInstruction || n != 0 {
		t.Fatal(n, err)
	}
	for _, instruction := range []DecoderInstruction{{InsertCountIncrement, 0}, {99, 1}, {StreamCancellation, MaxInteger + 1}} {
		dst := []byte{99}
		got, err := AppendDecoderInstruction(dst, instruction)
		if err == nil || !bytes.Equal(got, dst) {
			t.Fatal(got, err)
		}
	}
}
func FuzzInteger(f *testing.F) {
	f.Add([]byte{31, 154, 10}, uint8(5))
	f.Add([]byte{255, 0}, uint8(7))
	f.Fuzz(func(t *testing.T, b []byte, prefix uint8) {
		value, n, err := ConsumeInteger(b, prefix)
		if err != nil {
			if n != 0 {
				t.Fatal("error consumed input")
			}
			return
		}
		encoded, err := AppendInteger(nil, 0, prefix, value)
		if err != nil {
			t.Fatal(err)
		}
		again, used, err := ConsumeInteger(encoded, prefix)
		if err != nil || again != value || used != len(encoded) || n > len(b) || n > MaxIntegerBytes {
			t.Fatal("integer round trip")
		}
		instruction, n, err := ConsumeDecoderInstruction(b)
		if err == nil {
			canonical, err := AppendDecoderInstruction(nil, instruction)
			if err != nil {
				t.Fatal(err)
			}
			result, _, err := ConsumeDecoderInstruction(canonical)
			if err != nil || result != instruction || n > len(b) {
				t.Fatal("instruction round trip")
			}
		}
	})
}
