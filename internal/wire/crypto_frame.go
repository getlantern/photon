package wire

import (
	"errors"
	"io"
)

var ErrFrame = errors.New("quic: invalid frame encoding")

type CryptoFrame struct {
	Offset uint64
	Data   []byte
}

func (f CryptoFrame) Append(dst []byte) ([]byte, error) {
	if f.Offset > MaxVarint || uint64(len(f.Data)) > MaxVarint-f.Offset {
		return dst, ErrFrame
	}
	dst = append(dst, 0x06)
	dst, _ = AppendVarint(dst, f.Offset)
	dst, _ = AppendVarint(dst, uint64(len(f.Data)))
	return append(dst, f.Data...), nil
}

// ConsumeCryptoFrame parses exactly one CRYPTO frame and borrows its data.
// Frame types must use the shortest encoding (RFC 9000 §12.4).
func ConsumeCryptoFrame(b []byte) (CryptoFrame, int, error) {
	var f CryptoFrame
	if len(b) == 0 {
		return f, 0, io.ErrUnexpectedEOF
	}
	if b[0] != 0x06 {
		return f, 0, ErrFrame
	}
	offset, n, err := ConsumeVarint(b[1:])
	if err != nil {
		return f, 0, err
	}
	i := 1 + n
	length, n, err := ConsumeVarint(b[i:])
	if err != nil {
		return f, 0, err
	}
	i += n
	if length > MaxVarint-offset {
		return f, 0, ErrFrame
	}
	if length > uint64(len(b)-i) {
		return f, 0, io.ErrUnexpectedEOF
	}
	f.Offset, f.Data = offset, b[i:i+int(length)]
	return f, i + int(length), nil
}
