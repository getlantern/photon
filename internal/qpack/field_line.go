package qpack

import (
	"errors"
	"io"
)

type FieldLineType uint8

const (
	IndexedField FieldLineType = iota
	IndexedPostBase
	LiteralNameReference
	LiteralPostBase
	LiteralName
)

var ErrFieldLine = errors.New("qpack: invalid field line")

// FieldLine retains wire reference semantics until resolved against a section's
// Base. NeverIndex must be preserved when forwarding literals to another peer.
type FieldLine struct {
	Type                      FieldLineType
	Index                     uint64
	Static, NeverIndex        bool
	Name, Value               string
	HuffmanName, HuffmanValue bool
}

// ConsumeFieldLine parses one representation with a combined literal-byte cap.
// It does not resolve references or validate the section's Required Insert Count.
func ConsumeFieldLine(src []byte, maxLiteralBytes int) (FieldLine, int, error) {
	var v FieldLine
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
		v.Type, v.Static = IndexedField, src[0]&0x40 != 0
		v.Index, n, err = ConsumeInteger(src, 6)
	case src[0]&0x40 != 0:
		v.Type, v.Static, v.NeverIndex = LiteralNameReference, src[0]&0x10 != 0, src[0]&0x20 != 0
		v.Index, n, err = ConsumeInteger(src, 4)
	case src[0]&0x20 != 0:
		v.Type, v.NeverIndex, v.HuffmanName = LiteralName, src[0]&0x10 != 0, src[0]&8 != 0
		v.Name, n, err = ConsumeString(src, 3, maxLiteralBytes)
	case src[0]&0x10 != 0:
		v.Type = IndexedPostBase
		v.Index, n, err = ConsumeInteger(src, 4)
	default:
		v.Type, v.NeverIndex = LiteralPostBase, src[0]&8 != 0
		v.Index, n, err = ConsumeInteger(src, 3)
	}
	if err != nil {
		return FieldLine{}, 0, err
	}
	if v.Type == IndexedField || v.Type == IndexedPostBase {
		return v, n, nil
	}
	if n == len(src) {
		return FieldLine{}, 0, io.ErrUnexpectedEOF
	}
	v.HuffmanValue = src[n]&0x80 != 0
	value, m, err := ConsumeString(src[n:], 7, maxLiteralBytes-len(v.Name))
	if err != nil {
		return FieldLine{}, 0, err
	}
	v.Value = value
	return v, n + m, nil
}

func AppendFieldLine(dst []byte, v FieldLine) ([]byte, error) {
	if v.Type > LiteralName || ((v.Type == IndexedField || v.Type == IndexedPostBase) && v.NeverIndex) || ((v.Type == IndexedPostBase || v.Type == LiteralPostBase || v.Type == LiteralName) && v.Static) {
		return dst, ErrFieldLine
	}
	if v.Index > MaxInteger || uint64(len(v.Name)) > MaxInteger || uint64(len(v.Value)) > MaxInteger || (v.HuffmanName && HuffmanSize(v.Name) > MaxInteger) || (v.HuffmanValue && HuffmanSize(v.Value) > MaxInteger) {
		return dst, ErrIntegerOverflow
	}
	var flags byte
	var prefix uint8
	switch v.Type {
	case IndexedField:
		flags, prefix = 0x80, 6
		if v.Static {
			flags |= 0x40
		}
	case IndexedPostBase:
		flags, prefix = 0x10, 4
	case LiteralNameReference:
		flags, prefix = 0x40, 4
		if v.Static {
			flags |= 0x10
		}
		if v.NeverIndex {
			flags |= 0x20
		}
	case LiteralPostBase:
		prefix = 3
		if v.NeverIndex {
			flags |= 8
		}
	case LiteralName:
		flags = 0x20
		if v.NeverIndex {
			flags |= 0x10
		}
	}
	var out []byte
	if v.Type == LiteralName {
		out, _ = AppendString(dst, flags, 3, v.Name, v.HuffmanName)
	} else {
		out, _ = AppendInteger(dst, flags, prefix, v.Index)
	}
	if v.Type == IndexedField || v.Type == IndexedPostBase {
		return out, nil
	}
	out, _ = AppendString(out, 0, 7, v.Value, v.HuffmanValue)
	return out, nil
}

// ResolveFieldLine validates and resolves a reference without changing the table.
// required is the section's reconstructed Required Insert Count. It returns the
// dynamic absolute index plus one (or zero) for exact section-count validation.
// A caller must handle blocked sections before resolving their field lines.
func (t *DecoderTable) ResolveFieldLine(v FieldLine, base, required uint64) (Field, uint64, error) {
	if base > MaxInteger || required > MaxInteger || v.Index > MaxInteger {
		return Field{}, 0, ErrIntegerOverflow
	}
	if v.Type > LiteralName || ((v.Type == IndexedPostBase || v.Type == LiteralPostBase || v.Type == LiteralName) && v.Static) || ((v.Type == IndexedField || v.Type == IndexedPostBase) && v.NeverIndex) {
		return Field{}, 0, ErrFieldLine
	}
	if v.Type == LiteralName {
		return Field{v.Name, v.Value}, 0, nil
	}
	var f Field
	var ref uint64
	var err error
	if v.Static {
		f, err = Static(v.Index)
	} else {
		var id uint64
		if v.Type == IndexedPostBase || v.Type == LiteralPostBase {
			if v.Index > MaxInteger-base {
				return Field{}, 0, ErrTableIndex
			}
			id = base + v.Index
		} else {
			if v.Index >= base {
				return Field{}, 0, ErrTableIndex
			}
			id = base - v.Index - 1
		}
		if id >= required {
			return Field{}, 0, ErrTableIndex
		}
		f, err = t.Absolute(id)
		ref = id + 1
	}
	if err != nil {
		return Field{}, 0, err
	}
	if v.Type == LiteralNameReference || v.Type == LiteralPostBase {
		f.Value = v.Value
	}
	return f, ref, nil
}
