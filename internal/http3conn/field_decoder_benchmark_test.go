package http3conn

import (
	"io"
	"testing"

	"github.com/getlantern/photon/internal/qpack"
)

// BenchmarkFieldDecoderFeedback decodes a section that owes an acknowledgment
// and then cancels its stream: the two feedback instructions of a request.
func BenchmarkFieldDecoderFeedback(b *testing.B) {
	limits := decoderLimits()
	decoder, err := NewFieldDecoder(limits, io.Discard)
	if err != nil {
		b.Fatal(err)
	}
	defer decoder.Close(nil)
	var updates []byte
	for _, update := range []qpack.EncoderInstruction{{Type: qpack.SetDynamicCapacity, Index: 128}, {Type: qpack.InsertLiteralName, Name: ":status", Value: "200"}} {
		wire, err := qpack.AppendEncoderInstruction(nil, update)
		if err != nil {
			b.Fatal(err)
		}
		if err := decoder.EncoderInstruction(wire); err != nil {
			b.Fatal(err)
		}
		updates = append(updates, wire...)
	}
	section := []byte{2, 0, 0x80}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := uint64(i) * 4
		result, err := decoder.Submit(id, section)
		if err != nil {
			b.Fatal(err)
		}
		if decoded := <-result; decoded.Err != nil {
			b.Fatal(decoded.Err)
		}
		if err := decoder.Cancel(id); err != nil {
			b.Fatal(err)
		}
		// No request path produces feedback this fast; let the writer keep up.
		if i%1024 == 1023 {
			decoder.Flushed()
		}
	}
}
