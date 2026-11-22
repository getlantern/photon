package wire

import "io"

// StreamFrame owns no state. Stream direction, limits, and final-size consistency
// are enforced by the connection. Data from ConsumeStreamFrame borrows the input.
type StreamFrame struct {
	StreamID uint64
	Offset   uint64
	Data     []byte
	Fin      bool
	// OmitLength consumes the remainder of the packet; no frame may follow it.
	OmitLength bool
}

func (f StreamFrame) Append(dst []byte) ([]byte, error) {
	if f.StreamID > MaxVarint || f.Offset > MaxVarint || uint64(len(f.Data)) > MaxVarint-f.Offset {
		return dst, ErrFrame
	}
	typ := byte(0x08)
	if f.Fin {
		typ |= 1
	}
	if !f.OmitLength {
		typ |= 2
	}
	if f.Offset != 0 {
		typ |= 4
	}
	dst = append(dst, typ)
	dst, _ = AppendVarint(dst, f.StreamID)
	if f.Offset != 0 {
		dst, _ = AppendVarint(dst, f.Offset)
	}
	if !f.OmitLength {
		dst, _ = AppendVarint(dst, uint64(len(f.Data)))
	}
	return append(dst, f.Data...), nil
}

func ConsumeStreamFrame(b []byte) (StreamFrame, int, error) {
	var f StreamFrame
	if len(b) == 0 {
		return f, 0, io.ErrUnexpectedEOF
	}
	if b[0] < 8 || b[0] > 15 {
		return f, 0, ErrFrame
	}
	f.Fin = b[0]&1 != 0
	f.OmitLength = b[0]&2 == 0
	id, n, err := ConsumeVarint(b[1:])
	if err != nil {
		return StreamFrame{}, 0, err
	}
	f.StreamID = id
	i := 1 + n
	if b[0]&4 != 0 {
		f.Offset, n, err = ConsumeVarint(b[i:])
		if err != nil {
			return StreamFrame{}, 0, err
		}
		i += n
	}
	length := uint64(len(b) - i)
	if !f.OmitLength {
		length, n, err = ConsumeVarint(b[i:])
		if err != nil {
			return StreamFrame{}, 0, err
		}
		i += n
		if length > uint64(len(b)-i) {
			return StreamFrame{}, 0, io.ErrUnexpectedEOF
		}
	}
	if length > MaxVarint-f.Offset {
		return StreamFrame{}, 0, ErrFrame
	}
	f.Data = b[i : i+int(length)]
	return f, i + int(length), nil
}
