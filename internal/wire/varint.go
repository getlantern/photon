// Package wire implements QUIC wire encodings without connection state.
package wire

import (
	"encoding/binary"
	"errors"
	"io"
)

const MaxVarint uint64 = 1<<62 - 1

var ErrVarintRange = errors.New("quic: integer exceeds 62 bits")

// VarintLen returns the shortest encoding length, or zero for an invalid value.
func VarintLen(v uint64) int {
	switch {
	case v < 1<<6:
		return 1
	case v < 1<<14:
		return 2
	case v < 1<<30:
		return 4
	case v <= MaxVarint:
		return 8
	default:
		return 0
	}
}

// AppendVarint appends the shortest encoding. On error, dst is unchanged.
func AppendVarint(dst []byte, v uint64) ([]byte, error) {
	switch VarintLen(v) {
	case 1:
		return append(dst, byte(v)), nil
	case 2:
		return binary.BigEndian.AppendUint16(dst, uint16(v)|0x4000), nil
	case 4:
		return binary.BigEndian.AppendUint32(dst, uint32(v)|0x80000000), nil
	case 8:
		return binary.BigEndian.AppendUint64(dst, v|0xc000000000000000), nil
	default:
		return dst, ErrVarintRange
	}
}

// ConsumeVarint accepts non-minimal encodings as required by RFC 9000 §16.
// Errors consume no input.
func ConsumeVarint(b []byte) (uint64, int, error) {
	if len(b) == 0 {
		return 0, 0, io.ErrUnexpectedEOF
	}
	n := 1 << (b[0] >> 6)
	if len(b) < n {
		return 0, 0, io.ErrUnexpectedEOF
	}
	v := uint64(b[0] & 0x3f)
	for _, x := range b[1:n] {
		v = v<<8 | uint64(x)
	}
	return v, n, nil
}
