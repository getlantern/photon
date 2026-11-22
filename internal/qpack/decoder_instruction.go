package qpack

import (
	"errors"
	"io"
)

type DecoderInstructionType uint8

const (
	SectionAcknowledgment DecoderInstructionType = iota
	StreamCancellation
	InsertCountIncrement
)

var ErrInstruction = errors.New("qpack: invalid instruction")

// Value is a stream ID for acknowledgments/cancellations and a nonzero count
// for increments. Validation against dynamic table/blocked-section state is
// performed by the future encoder, not by this wire codec.
type DecoderInstruction struct {
	Type  DecoderInstructionType
	Value uint64
}

func ConsumeDecoderInstruction(src []byte) (DecoderInstruction, int, error) {
	if len(src) == 0 {
		return DecoderInstruction{}, 0, io.ErrUnexpectedEOF
	}
	typ, prefix := InsertCountIncrement, uint8(6)
	if src[0]&128 != 0 {
		typ, prefix = SectionAcknowledgment, 7
	} else if src[0]&64 != 0 {
		typ = StreamCancellation
	}
	value, n, err := ConsumeInteger(src, prefix)
	if err != nil {
		return DecoderInstruction{}, 0, err
	}
	if typ == InsertCountIncrement && value == 0 {
		return DecoderInstruction{}, 0, ErrInstruction
	}
	return DecoderInstruction{typ, value}, n, nil
}
func AppendDecoderInstruction(dst []byte, instruction DecoderInstruction) ([]byte, error) {
	var flags byte
	prefix := uint8(6)
	switch instruction.Type {
	case SectionAcknowledgment:
		flags, prefix = 128, 7
	case StreamCancellation:
		flags = 64
	case InsertCountIncrement:
		if instruction.Value == 0 {
			return dst, ErrInstruction
		}
	default:
		return dst, ErrInstruction
	}
	return AppendInteger(dst, flags, prefix, instruction.Value)
}
