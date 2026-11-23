package qpack

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
	"testing/iotest"
)

func TestFeedbackIO(t *testing.T) {
	values := []DecoderInstruction{{Type: SectionAcknowledgment, Value: 0}, {Type: StreamCancellation, Value: MaxInteger}, {Type: InsertCountIncrement, Value: 1}, {Type: SectionAcknowledgment, Value: 128}}
	var buf bytes.Buffer
	writer, _ := NewFeedbackWriter(&buf)
	n, err := writer.Write(values)
	if err != nil || n != len(values) {
		t.Fatal(n, err)
	}
	source := bytes.NewReader(buf.Bytes())
	reader, _ := NewDecoderInstructionReader(iotest.OneByteReader(source))
	for _, want := range values {
		before := source.Len()
		got, err := reader.Next()
		if err != nil || got != want {
			t.Fatal(got, err)
		}
		wire, _ := AppendDecoderInstruction(nil, want)
		if before-source.Len() != len(wire) {
			t.Fatal("overread")
		}
		for cut := 1; cut < len(wire); cut++ {
			truncated, _ := NewDecoderInstructionReader(bytes.NewReader(wire[:cut]))
			if _, err := truncated.Next(); !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatal("truncation", err)
			}
		}
	}
	if _, err := reader.Next(); err != io.EOF {
		t.Fatal(err)
	}
	for _, wire := range [][]byte{{0}, append([]byte{0xff}, bytes.Repeat([]byte{0xff}, 15)...)} {
		reader, _ := NewDecoderInstructionReader(bytes.NewReader(wire))
		_, err := reader.Next()
		if err == nil {
			t.Fatal("invalid feedback")
		}
		if _, again := reader.Next(); again != err {
			t.Fatal("not sticky")
		}
	}
	failure := errors.New("reset")
	reader, _ = NewDecoderInstructionReader(io.MultiReader(bytes.NewReader([]byte{0xff}), iotest.ErrReader(failure)))
	if _, err := reader.Next(); err != failure {
		t.Fatal(err)
	}
}

type feedbackTestWriter struct {
	n     int
	err   error
	calls int
}

func (w *feedbackTestWriter) Write(p []byte) (int, error) { w.calls++; return w.n, w.err }
func TestFeedbackWriteFailures(t *testing.T) {
	failure := errors.New("write reset")
	batch := []DecoderInstruction{{Type: SectionAcknowledgment, Value: 128}}
	for _, c := range []struct {
		n     int
		err   error
		count int
	}{{0, nil, 0}, {1, nil, 0}, {1, failure, 0}, {2, failure, 1}, {3, nil, 0}, {-1, nil, 0}} {
		target := &feedbackTestWriter{n: c.n, err: c.err}
		writer, _ := NewFeedbackWriter(target)
		count, err := writer.Write(batch)
		if err == nil || count != c.count {
			t.Fatal(count, err)
		}
		if c.err != nil && err != c.err {
			t.Fatal("cause", err)
		}
		if _, again := writer.Write(batch); again != err || target.calls != 1 {
			t.Fatal("retried failed write", again, target.calls)
		}
	}
	var buf bytes.Buffer
	writer, _ := NewFeedbackWriter(&buf)
	bad := append(append([]DecoderInstruction(nil), batch...), DecoderInstruction{Type: InsertCountIncrement})
	if n, err := writer.Write(bad); err == nil || n != 0 || buf.Len() != 0 {
		t.Fatal("partial validation", n, err)
	}
	if n, err := writer.Write(batch); err != nil || n != 1 {
		t.Fatal("local error poisoned writer", n, err)
	}
}
func TestDecoderFeedbackDelivery(t *testing.T) {
	d := decoderForTest(t)
	_, _, blocked, err := d.Submit(4, []byte{2, 0, 0x80})
	if err != nil || !blocked {
		t.Fatal(err)
	}
	var dst bytes.Buffer
	writer, _ := NewFeedbackWriter(&dst)
	for _, v := range []EncoderInstruction{{Type: SetDynamicCapacity, Index: 128}, {Type: InsertLiteralName, Name: "a", Value: "b"}, {Type: DuplicateEntry}} {
		_, instructions := feedDecoder(t, d, v)
		if _, err := writer.Write(instructions); err != nil {
			t.Fatal(err)
		}
	}
	instructions, err := d.Cancel(8)
	if err != nil {
		t.Fatal(err)
	}
	writer.Write(instructions)
	reader, _ := NewDecoderInstructionReader(bytes.NewReader(dst.Bytes()))
	var got []DecoderInstruction
	for {
		v, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	want := []DecoderInstruction{{Type: SectionAcknowledgment, Value: 4}, {Type: InsertCountIncrement, Value: 1}, {Type: StreamCancellation, Value: 8}}
	if !reflect.DeepEqual(got, want) {
		t.Fatal(got)
	}
}
func FuzzFeedbackReader(f *testing.F) {
	f.Add([]byte{0x80, 0x40, 1})
	f.Add([]byte{0xff, 0x80, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		source := bytes.NewReader(data)
		reader, _ := NewDecoderInstructionReader(iotest.OneByteReader(source))
		offset := 0
		for {
			got, err := reader.Next()
			want, n, parseErr := ConsumeDecoderInstruction(data[offset:])
			if err != nil {
				if parseErr == nil {
					t.Fatal("valid instruction rejected", err)
				}
				break
			}
			if parseErr != nil || got != want || len(data)-source.Len() != offset+n {
				t.Fatal("framing mismatch")
			}
			offset += n
		}
	})
}
