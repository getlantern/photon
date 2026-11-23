package wire

import "io"

// AckRange is inclusive. AckFrame ranges are strictly descending and separated
// by at least one unacknowledged packet.
type AckRange struct{ Smallest, Largest uint64 }

type AckFrame struct {
	// Delay is the encoded value, before applying the peer's ACK delay exponent.
	Delay  uint64
	Ranges []AckRange
	// ECN contains ECT(0), ECT(1), CE, in that order. Nil means no ECN section.
	ECN *[3]uint64
}

func (f AckFrame) Append(dst []byte) ([]byte, error) {
	return f.appendPrefix(dst, f.Type())
}

// appendPrefix validates the whole ACK before writing any prefix or body.
func (f AckFrame) appendPrefix(dst []byte, typ uint64, prefix ...uint64) ([]byte, error) {
	for _, v := range prefix {
		if v > MaxVarint {
			return dst, ErrFrame
		}
	}
	if len(f.Ranges) == 0 || f.Delay > MaxVarint {
		return dst, ErrFrame
	}
	for i, r := range f.Ranges {
		if r.Largest > MaxVarint || r.Smallest > r.Largest {
			return dst, ErrFrame
		}
		if i > 0 && f.Ranges[i-1].Smallest < r.Largest+2 {
			return dst, ErrFrame
		}
	}
	if f.ECN != nil {
		for _, count := range f.ECN {
			if count > MaxVarint {
				return dst, ErrFrame
			}
		}
	}
	dst, _ = appendFields(dst, typ, prefix...)
	dst, _ = AppendVarint(dst, f.Ranges[0].Largest)
	dst, _ = AppendVarint(dst, f.Delay)
	dst, _ = AppendVarint(dst, uint64(len(f.Ranges)-1))
	dst, _ = AppendVarint(dst, f.Ranges[0].Largest-f.Ranges[0].Smallest)
	for i := 1; i < len(f.Ranges); i++ {
		dst, _ = AppendVarint(dst, f.Ranges[i-1].Smallest-f.Ranges[i].Largest-2)
		dst, _ = AppendVarint(dst, f.Ranges[i].Largest-f.Ranges[i].Smallest)
	}
	if f.ECN != nil {
		for _, count := range f.ECN {
			dst, _ = AppendVarint(dst, count)
		}
	}
	return dst, nil
}

func ConsumeAckFrame(b []byte) (AckFrame, int, error) {
	var f AckFrame
	if len(b) == 0 {
		return f, 0, io.ErrUnexpectedEOF
	}
	if b[0] != 2 && b[0] != 3 {
		return f, 0, ErrFrame
	}
	return consumeAckBody(b, 1, b[0] == 3)
}

func consumeAckBody(b []byte, start int, ecn bool) (AckFrame, int, error) {
	var f AckFrame
	i := start
	read := func() (uint64, error) {
		v, n, err := ConsumeVarint(b[i:])
		if err == nil {
			i += n
		}
		return v, err
	}
	largest, err := read()
	if err != nil {
		return AckFrame{}, 0, err
	}
	f.Delay, err = read()
	if err != nil {
		return AckFrame{}, 0, err
	}
	count, err := read()
	if err != nil {
		return AckFrame{}, 0, err
	}
	first, err := read()
	if err != nil {
		return AckFrame{}, 0, err
	}
	if first > largest {
		return AckFrame{}, 0, ErrFrame
	}
	// Each additional range requires at least two input bytes. Never allocate
	// based on the peer's count alone (it can be as large as 2^62-1).
	if count > uint64((len(b)-i)/2) {
		return AckFrame{}, 0, io.ErrUnexpectedEOF
	}
	f.Ranges = make([]AckRange, 0, int(count)+1)
	f.Ranges = append(f.Ranges, AckRange{Smallest: largest - first, Largest: largest})
	for j := uint64(0); j < count; j++ {
		gap, err := read()
		if err != nil {
			return AckFrame{}, 0, err
		}
		length, err := read()
		if err != nil {
			return AckFrame{}, 0, err
		}
		previous := f.Ranges[len(f.Ranges)-1].Smallest
		if previous < 2 || gap > previous-2 {
			return AckFrame{}, 0, ErrFrame
		}
		largest = previous - gap - 2
		if length > largest {
			return AckFrame{}, 0, ErrFrame
		}
		f.Ranges = append(f.Ranges, AckRange{Smallest: largest - length, Largest: largest})
	}
	if ecn {
		f.ECN = new([3]uint64)
		for j := range f.ECN {
			f.ECN[j], err = read()
			if err != nil {
				return AckFrame{}, 0, err
			}
		}
	}
	return f, i, nil
}

// Acknowledges searches validated descending ACK ranges in logarithmic time.
func (f AckFrame) Acknowledges(pn uint64) bool {
	low, high := 0, len(f.Ranges)
	for low < high {
		mid := low + (high-low)/2
		if f.Ranges[mid].Smallest <= pn {
			high = mid
		} else {
			low = mid + 1
		}
	}
	return low < len(f.Ranges) && pn <= f.Ranges[low].Largest
}
