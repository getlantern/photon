package qpack

import "io"

type EncoderInstructionType uint8

const (
	SetDynamicCapacity EncoderInstructionType = iota
	InsertNameReference
	InsertLiteralName
	DuplicateEntry
)

// Index is a capacity, static index or encoder-relative dynamic index according
// to Type. Name is used only for InsertLiteralName. Huffman flags select output
// encoding; decoded instructions contain the observed flags.
type EncoderInstruction struct {
	Type                      EncoderInstructionType
	Index                     uint64
	Static                    bool
	Name, Value               string
	HuffmanName, HuffmanValue bool
}

// ConsumeEncoderInstruction parses one instruction without touching table state.
// maxLiteralBytes bounds the sum of decoded literal name and value bytes.
func ConsumeEncoderInstruction(src []byte, maxLiteralBytes int) (EncoderInstruction, int, error) {
	var v EncoderInstruction
	if maxLiteralBytes < 0 {
		return v, 0, ErrStringLimit
	}
	if len(src) == 0 {
		return v, 0, io.ErrUnexpectedEOF
	}
	var n int
	var err error
	switch {
	case src[0]&0x80 != 0:
		v.Type, v.Static = InsertNameReference, src[0]&0x40 != 0
		v.Index, n, err = ConsumeInteger(src, 6)
	case src[0]&0x40 != 0:
		v.Type, v.HuffmanName = InsertLiteralName, src[0]&0x20 != 0
		v.Name, n, err = ConsumeString(src, 5, maxLiteralBytes)
	case src[0]&0x20 != 0:
		v.Type = SetDynamicCapacity
		v.Index, n, err = ConsumeInteger(src, 5)
	default:
		v.Type = DuplicateEntry
		v.Index, n, err = ConsumeInteger(src, 5)
	}
	if err != nil {
		return EncoderInstruction{}, 0, err
	}
	if v.Type == InsertNameReference || v.Type == InsertLiteralName {
		if n == len(src) {
			return EncoderInstruction{}, 0, io.ErrUnexpectedEOF
		}
		v.HuffmanValue = src[n]&0x80 != 0
		var m int
		v.Value, m, err = ConsumeString(src[n:], 7, maxLiteralBytes-len(v.Name))
		if err != nil {
			return EncoderInstruction{}, 0, err
		}
		n += m
	}
	return v, n, nil
}

func AppendEncoderInstruction(dst []byte, v EncoderInstruction) ([]byte, error) {
	// Validate all potentially failing values before appending to caller storage.
	if v.Index > MaxInteger || uint64(len(v.Name)) > MaxInteger || uint64(len(v.Value)) > MaxInteger {
		return dst, ErrIntegerOverflow
	}
	if (v.HuffmanName && HuffmanSize(v.Name) > MaxInteger) || (v.HuffmanValue && HuffmanSize(v.Value) > MaxInteger) {
		return dst, ErrIntegerOverflow
	}
	var out []byte
	var err error
	switch v.Type {
	case SetDynamicCapacity:
		return AppendInteger(dst, 0x20, 5, v.Index)
	case DuplicateEntry:
		return AppendInteger(dst, 0, 5, v.Index)
	case InsertNameReference:
		flags := byte(0x80)
		if v.Static {
			flags |= 0x40
		}
		out, err = AppendInteger(dst, flags, 6, v.Index)
	case InsertLiteralName:
		out, err = AppendString(dst, 0x40, 5, v.Name, v.HuffmanName)
	default:
		return dst, ErrInstruction
	}
	if err != nil {
		return dst, err
	}
	out, err = AppendString(out, 0, 7, v.Value, v.HuffmanValue)
	if err != nil {
		return dst, err
	}
	return out, nil
}

// ApplyEncoderInstruction applies exactly one instruction atomically. Insertion
// references are resolved before eviction, including references to the victim.
// Wire/stream errors must be mapped to QPACK_ENCODER_STREAM_ERROR by the owner.
func (t *DecoderTable) ApplyEncoderInstruction(v EncoderInstruction) error {
	switch v.Type {
	case SetDynamicCapacity:
		return t.SetCapacity(v.Index)
	case DuplicateEntry:
		_, err := t.Duplicate(v.Index)
		return err
	case InsertLiteralName:
		_, err := t.Insert(Field{v.Name, v.Value})
		return err
	case InsertNameReference:
		var f Field
		var err error
		if v.Static {
			f, err = Static(v.Index)
		} else {
			f, err = t.Relative(v.Index)
		}
		if err != nil {
			return err
		}
		_, err = t.Insert(Field{f.Name, v.Value})
		return err
	default:
		return ErrInstruction
	}
}

// ConsumeEncoderUpdate parses and applies one complete instruction. On any
// error it consumes no input and preserves the dynamic table.
func (t *DecoderTable) ConsumeEncoderUpdate(src []byte, maxLiteralBytes int) (int, error) {
	v, n, err := ConsumeEncoderInstruction(src, maxLiteralBytes)
	if err != nil {
		return 0, err
	}
	if err = t.ApplyEncoderInstruction(v); err != nil {
		return 0, err
	}
	return n, nil
}
