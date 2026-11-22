package qpack

import (
	"errors"
	"io"
)

// DecoderInstructionReader reads feedback after the decoder stream type. It uses
// fixed storage and does not read beyond one instruction. Its owner serializes
// calls and maps stream closure/reset to HTTP/3 critical-stream errors.
type DecoderInstructionReader struct {
	source io.Reader
	err    error
}

func NewDecoderInstructionReader(source io.Reader) (*DecoderInstructionReader, error) {
	if source == nil {
		return nil, ErrDecoderConfig
	}
	return &DecoderInstructionReader{source: source}, nil
}
func (r *DecoderInstructionReader) Next() (DecoderInstruction, error) {
	if r.err != nil {
		return DecoderInstruction{}, r.err
	}
	var buf [MaxIntegerBytes]byte
	for n := 1; n <= len(buf); n++ {
		_, err := io.ReadFull(r.source, buf[n-1:n])
		if err != nil {
			if errors.Is(err, io.EOF) && n > 1 {
				err = io.ErrUnexpectedEOF
			}
			r.err = err
			return DecoderInstruction{}, err
		}
		v, _, err := ConsumeDecoderInstruction(buf[:n])
		if err == nil {
			return v, nil
		}
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			r.err = err
			return DecoderInstruction{}, err
		}
	}
	r.err = ErrIntegerLength
	return DecoderInstruction{}, r.err
}

// FeedbackWriter sends feedback in caller order, using fixed-size scratch space.
// The owner must serialize calls in the same order as Decoder results, provide
// stream deadlines/cancellation, and handle write failures as connection errors.
// It never closes the writer, emits a stream-type prefix, or retains a queue.
type FeedbackWriter struct {
	destination io.Writer
	err         error
}

func NewFeedbackWriter(destination io.Writer) (*FeedbackWriter, error) {
	if destination == nil {
		return nil, ErrDecoderConfig
	}
	return &FeedbackWriter{destination: destination}, nil
}

// Write validates the entire batch before sending anything. The returned count
// is the number of complete encoded instructions accepted by the writer. A
// write error is sticky, even if all bytes were accepted, because retrying could
// duplicate a partially or completely transmitted acknowledgment. Short writes
// without errors are treated as io.ErrShortWrite. Local validation errors leave
// the writer usable and transmit nothing.
func (w *FeedbackWriter) Write(instructions []DecoderInstruction) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	var scratch [MaxIntegerBytes]byte
	for _, v := range instructions {
		if _, err := AppendDecoderInstruction(scratch[:0], v); err != nil {
			return 0, err
		}
	}
	complete := 0
	for _, v := range instructions {
		encoded, _ := AppendDecoderInstruction(scratch[:0], v)
		n, err := w.destination.Write(encoded)
		if n < 0 || n > len(encoded) {
			err = io.ErrShortWrite
		} else if n == len(encoded) {
			complete++
		} else if err == nil {
			err = io.ErrShortWrite
		}
		if err != nil {
			w.err = err
			return complete, err
		}
	}
	return complete, nil
}
