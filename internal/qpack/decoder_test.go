package qpack

import (
	"errors"
	"io"
	"reflect"
	"testing"
)

func decoderForTest(t *testing.T) *Decoder {
	t.Helper()
	d, err := NewDecoder(DecoderLimits{MaxTableCapacity: 128, MaxBlockedStreams: 2, MaxBufferedBytes: 64, MaxEncodedSection: 64, MaxDecodedSection: 1024, MaxFields: 16, MaxInstructionLiteralBytes: 96})
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func feedDecoder(t *testing.T, d *Decoder, v EncoderInstruction) ([]SectionResult, []DecoderInstruction) {
	t.Helper()
	wire, err := AppendEncoderInstruction(nil, v)
	if err != nil {
		t.Fatal(err)
	}
	for cut := 0; cut < len(wire); cut++ {
		n, r, i, err := d.ConsumeEncoder(wire[:cut])
		if !errors.Is(err, io.ErrUnexpectedEOF) || n != 0 || len(r) != 0 || len(i) != 0 {
			t.Fatalf("fragment %d: %d %v", cut, n, err)
		}
	}
	n, r, i, err := d.ConsumeEncoder(wire)
	if err != nil || n != len(wire) {
		t.Fatal(n, err)
	}
	return r, i
}
func TestDecoderBlockResumeAndAcknowledgment(t *testing.T) {
	d := decoderForTest(t)
	feedDecoder(t, d, EncoderInstruction{Type: SetDynamicCapacity, Index: 128})
	wire := []byte{2, 0, 0x80} // RIC=1, Base=1, relative zero.
	for _, id := range []uint64{8, 4} {
		fields, instructions, blocked, err := d.Submit(id, wire)
		if err != nil || !blocked || fields != nil || instructions != nil {
			t.Fatal(fields, instructions, blocked, err)
		}
	}
	if d.BlockedStreams() != 2 || d.BufferedBytes() != 6 {
		t.Fatal("accounting")
	}
	wire[2] = 0xff // Decoder owns its blocked payloads.
	if _, _, _, err := d.Submit(4, []byte{0, 0}); !errors.Is(err, ErrPendingSection) {
		t.Fatal("pending", err)
	}
	ready, instructions := feedDecoder(t, d, EncoderInstruction{Type: InsertLiteralName, Name: "a", Value: "b"})
	if len(ready) != 2 || ready[0].StreamID != 4 || ready[1].StreamID != 8 || ready[0].Fields[0].Field != (Field{"a", "b"}) {
		t.Fatal(ready)
	}
	want := []DecoderInstruction{{Type: SectionAcknowledgment, Value: 4}, {Type: SectionAcknowledgment, Value: 8}}
	if !reflect.DeepEqual(instructions, want) || d.BlockedStreams() != 0 || d.BufferedBytes() != 0 {
		t.Fatal(instructions, d.buffered)
	}
	// Every subsequent dynamic section gets its own ACK, even without new inserts.
	_, instructions, blocked, err := d.Submit(4, []byte{2, 0, 0x80})
	if err != nil || blocked || !reflect.DeepEqual(instructions, want[:1]) {
		t.Fatal(instructions, blocked, err)
	}
	_, instructions, _, err = d.Submit(4, []byte{0, 0, 0xd1})
	if err != nil || len(instructions) != 0 {
		t.Fatal("static ACK", instructions, err)
	}
	_, instructions = feedDecoder(t, d, EncoderInstruction{Type: DuplicateEntry})
	if !reflect.DeepEqual(instructions, []DecoderInstruction{{Type: InsertCountIncrement, Value: 1}}) {
		t.Fatal(instructions)
	}
}
func TestDecoderCancelAndSpeculativeInsert(t *testing.T) {
	d := decoderForTest(t)
	feedDecoder(t, d, EncoderInstruction{Type: SetDynamicCapacity, Index: 128})
	if _, _, blocked, err := d.Submit(4, []byte{2, 0, 0x80}); !blocked || err != nil {
		t.Fatal(err)
	}
	instructions, err := d.Cancel(4)
	if err != nil || !reflect.DeepEqual(instructions, []DecoderInstruction{{Type: StreamCancellation, Value: 4}}) || d.BufferedBytes() != 0 || d.BlockedStreams() != 0 {
		t.Fatal(instructions, err)
	}
	ready, instructions := feedDecoder(t, d, EncoderInstruction{Type: InsertLiteralName, Name: "a", Value: "b"})
	if len(ready) != 0 || !reflect.DeepEqual(instructions, []DecoderInstruction{{Type: InsertCountIncrement, Value: 1}}) {
		t.Fatal(ready, instructions)
	}
	if _, err := d.Cancel(12); err != nil {
		t.Fatal("reset before section", err)
	}
}
func TestDecoderLimitsAndTerminalFailure(t *testing.T) {
	for _, mode := range []string{"streams", "bytes", "encoded", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			d := decoderForTest(t)
			switch mode {
			case "streams":
				d.limits.MaxBlockedStreams = 0
			case "bytes":
				d.limits.MaxBufferedBytes = 2
			case "encoded":
				d.limits.MaxEncodedSection = 2
			}
			wire := []byte{2, 0, 0x80}
			if mode == "malformed" {
				wire = []byte{0}
			}
			_, _, _, err := d.Submit(4, wire)
			if err == nil {
				t.Fatal("accepted")
			}
			if _, _, _, next := d.Submit(8, []byte{0, 0}); next != err {
				t.Fatal("not terminal", next)
			}
			if _, _, _, next := d.ConsumeEncoder([]byte{0x20}); next != err {
				t.Fatal("not terminal", next)
			}
			if d.BufferedBytes() != 0 || d.BlockedStreams() != 0 {
				t.Fatal("retained buffer")
			}
		})
	}
	d := decoderForTest(t)
	feedDecoder(t, d, EncoderInstruction{Type: SetDynamicCapacity, Index: 128})
	// Malformed field bodies can be detected only when unblocked.
	d.Submit(4, []byte{2, 0, 0xff})
	wire, _ := AppendEncoderInstruction(nil, EncoderInstruction{Type: InsertLiteralName, Name: "a", Value: "b"})
	n, r, i, err := d.ConsumeEncoder(wire)
	if err == nil || n != len(wire) || r != nil || i != nil || d.BlockedStreams() != 0 {
		t.Fatal(n, r, i, err)
	}
}
func FuzzDecoderState(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	f.Fuzz(func(t *testing.T, data []byte) {
		d := decoderForTest(t)
		capWire, _ := AppendEncoderInstruction(nil, EncoderInstruction{Type: SetDynamicCapacity, Index: 128})
		d.ConsumeEncoder(capWire)
		for _, op := range data {
			id := uint64(op%4) * 4
			switch op % 3 {
			case 0:
				p := SectionPrefix{RequiredInsertCount: d.table.inserts + 1, Base: d.table.inserts + 1}
				wire, e := AppendSectionPrefix(nil, p, 128)
				if e != nil {
					return
				}
				wire = append(wire, 0x80)
				d.Submit(id, wire)
			case 1:
				d.Cancel(id)
			case 2:
				wire, _ := AppendEncoderInstruction(nil, EncoderInstruction{Type: InsertLiteralName, Name: "a", Value: "b"})
				d.ConsumeEncoder(wire)
			}
			if d.BufferedBytes() < 0 || d.BufferedBytes() > 64 || d.BlockedStreams() > 2 || d.reported > d.table.inserts {
				t.Fatal("bounds")
			}
			if d.err != nil {
				break
			}
		}
	})
}
