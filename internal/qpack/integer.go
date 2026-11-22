// Package qpack implements independent RFC 9204 compression primitives.
// Stream integration and encoder reference policy are unfinished.
package qpack

import (
	"errors"
	"io"
)

const MaxInteger uint64 = 1<<62 - 1

// MaxIntegerBytes bounds redundant encodings, as permitted by RFC 7541 section
// 5.1 and RFC 9204 section 7.4. All 62-bit values need at most ten bytes.
const MaxIntegerBytes = 16

var (
	ErrPrefix          = errors.New("qpack: invalid integer prefix or overlapping flags")
	ErrIntegerOverflow = errors.New("qpack: integer exceeds 62 bits")
	ErrIntegerLength   = errors.New("qpack: integer encoding too long")
)

// AppendInteger preserves flags above the prefix and emits a minimal integer.
// Flags must leave the prefix bits clear. Errors leave dst unchanged.
func AppendInteger(dst []byte, flags byte, prefix uint8, value uint64) ([]byte, error) {
	if prefix < 1 || prefix > 8 {
		return dst, ErrPrefix
	}
	mask := uint64(1<<prefix) - 1
	if uint64(flags)&mask != 0 {
		return dst, ErrPrefix
	}
	if value > MaxInteger {
		return dst, ErrIntegerOverflow
	}
	if value < mask {
		return append(dst, flags|byte(value)), nil
	}
	dst = append(dst, flags|byte(mask))
	value -= mask
	for value >= 128 {
		dst = append(dst, byte(value&127)|128)
		value >>= 7
	}
	return append(dst, byte(value)), nil
}

// ConsumeInteger ignores opcode bits above prefix. Errors consume no input,
// enabling callers to retry after receiving a fragmented instruction. Nonminimal
// encodings are accepted up to MaxIntegerBytes, with no integer overflow.
func ConsumeInteger(src []byte, prefix uint8) (uint64, int, error) {
	if prefix < 1 || prefix > 8 {
		return 0, 0, ErrPrefix
	}
	if len(src) == 0 {
		return 0, 0, io.ErrUnexpectedEOF
	}
	mask := uint64(1<<prefix) - 1
	value := uint64(src[0]) & mask
	if value < mask {
		return value, 1, nil
	}
	for i, shift := 1, uint(0); i < MaxIntegerBytes; i, shift = i+1, shift+7 {
		if i >= len(src) {
			return 0, 0, io.ErrUnexpectedEOF
		}
		part := uint64(src[i] & 127)
		if shift >= 62 {
			if part != 0 {
				return 0, 0, ErrIntegerOverflow
			}
		} else {
			if part > (MaxInteger-value)>>shift {
				return 0, 0, ErrIntegerOverflow
			}
			value += part << shift
		}
		if src[i]&128 == 0 {
			return value, i + 1, nil
		}
	}
	return 0, 0, ErrIntegerLength
}
