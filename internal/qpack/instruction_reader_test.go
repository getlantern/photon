package qpack

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"testing/iotest"
)

func TestEncoderInstructionReader(t *testing.T) {
	forms := []EncoderInstruction{
		{Type: SetDynamicCapacity, Index: 512},
		{Type: InsertLiteralName, Name: "custom-key", Value: "custom-value", HuffmanName: true, HuffmanValue: true},
		{Type: InsertNameReference, Static: true, Index: 98, Value: "value"},
		{Type: InsertNameReference, Index: 0, Value: ""},
		{Type: DuplicateEntry, Index: 255},
	}
	var combined []byte
	var wires [][]byte
	for _, v := range forms {
		wire, err := AppendEncoderInstruction(nil, v)
		if err != nil {
			t.Fatal(err)
		}
		wires = append(wires, wire)
		combined = append(combined, wire...)
	}
	for _, fragmented := range []bool{false, true} {
		var source io.Reader = bytes.NewReader(combined)
		if fragmented {
			source = iotest.OneByteReader(source)
		}
		reader, _ := NewEncoderInstructionReader(source, 128)
		for i, want := range wires {
			got, err := reader.Next()
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("%d: %x %v", i, got, err)
			}
			v, n, err := ConsumeEncoderInstruction(got, 128)
			if err != nil || v != forms[i] || n != len(got) {
				t.Fatal(v, n, err)
			}
		}
		if _, err := reader.Next(); err != io.EOF {
			t.Fatal("boundary EOF", err)
		}
	}
	for _, wire := range wires {
		for cut := 1; cut < len(wire); cut++ {
			reader, _ := NewEncoderInstructionReader(bytes.NewReader(wire[:cut]), 128)
			if _, err := reader.Next(); !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("%x cut %d: %v", wire, cut, err)
			}
		}
		source := bytes.NewReader(append(bytes.Clone(wire), 0x20))
		reader, _ := NewEncoderInstructionReader(source, len(wire))
		got, err := reader.Next()
		if err != nil || !bytes.Equal(got, wire) || source.Len() != 1 {
			t.Fatal("boundary", got, err, source.Len())
		}
	}
}
func TestEncoderInstructionReaderLimits(t *testing.T) {
	// Advertised string length exceeds cap: reject before reading body.
	source := bytes.NewReader([]byte{0x41, 'a', 0x7f, 0xff, 0xff, 0x7f, 'x'})
	reader, _ := NewEncoderInstructionReader(source, 64)
	_, err := reader.Next()
	if !errors.Is(err, ErrInstructionLimit) || source.Len() != 1 || reader.buf != nil {
		t.Fatal(err, source.Len())
	}
	if _, again := reader.Next(); again != err {
		t.Fatal("not sticky")
	}
	wire := append([]byte{0x3f}, bytes.Repeat([]byte{0x80}, MaxIntegerBytes)...)
	reader, _ = NewEncoderInstructionReader(bytes.NewReader(wire), 64)
	if _, err := reader.Next(); !errors.Is(err, ErrIntegerLength) {
		t.Fatal(err)
	}
	failure := errors.New("source reset")
	reader, _ = NewEncoderInstructionReader(io.MultiReader(bytes.NewReader([]byte{0x41}), iotest.ErrReader(failure)), 64)
	if _, err := reader.Next(); err != failure {
		t.Fatal("source cause lost", err)
	}
}
func FuzzEncoderInstructionReader(f *testing.F) {
	f.Add([]byte{0x41, 'a', 1, 'b', 0x20})
	f.Add([]byte{0x3f, 0xbd, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		reader, _ := NewEncoderInstructionReader(iotest.OneByteReader(bytes.NewReader(data)), 256)
		consumed := 0
		for {
			wire, err := reader.Next()
			if err != nil {
				break
			}
			if cap(reader.buf) > 256 || len(wire) == 0 || len(wire) > 256 || !bytes.Equal(wire, data[consumed:consumed+len(wire)]) {
				t.Fatal("framing")
			}
			_, n, err := ConsumeEncoderInstruction(wire, 1024)
			// Framing does not validate Huffman contents or decoded resource limits.
			if err == nil && n != len(wire) {
				t.Fatal("multiple instructions")
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatal("truncated framing")
			}
			consumed += len(wire)
		}
	})
}

func TestEncoderInstructionReaderFeedsDecoder(t *testing.T) {
	d := decoderForTest(t)
	if _, _, blocked, err := d.Submit(4, []byte{2, 0, 0x80}); err != nil || !blocked {
		t.Fatal(blocked, err)
	}
	var stream []byte
	for _, v := range []EncoderInstruction{{Type: SetDynamicCapacity, Index: 128}, {Type: InsertLiteralName, Name: "a", Value: "b"}, {Type: DuplicateEntry}} {
		var err error
		stream, err = AppendEncoderInstruction(stream, v)
		if err != nil {
			t.Fatal(err)
		}
	}
	reader, _ := NewEncoderInstructionReader(iotest.OneByteReader(bytes.NewReader(stream)), 64)
	for step := 0; step < 3; step++ {
		wire, err := reader.Next()
		if err != nil {
			t.Fatal(err)
		}
		n, ready, instructions, err := d.ConsumeEncoder(wire)
		if err != nil || n != len(wire) {
			t.Fatal(n, err)
		}
		switch step {
		case 0:
			if len(ready) != 0 || len(instructions) != 0 {
				t.Fatal(ready, instructions)
			}
		case 1:
			if len(ready) != 1 || ready[0].StreamID != 4 || ready[0].Fields[0].Field != (Field{"a", "b"}) || len(instructions) != 1 || instructions[0] != (DecoderInstruction{Type: SectionAcknowledgment, Value: 4}) {
				t.Fatal(ready, instructions)
			}
		case 2:
			if len(ready) != 0 || len(instructions) != 1 || instructions[0] != (DecoderInstruction{Type: InsertCountIncrement, Value: 1}) {
				t.Fatal(ready, instructions)
			}
		}
	}
	if _, err := reader.Next(); err != io.EOF {
		t.Fatal(err)
	}
}
