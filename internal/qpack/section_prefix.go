package qpack

import "errors"

var ErrSectionPrefix = errors.New("qpack: invalid field-section prefix")

// SectionPrefix contains reconstructed absolute counts. A RequiredInsertCount
// greater than the decoder's insertion count means blocked, not malformed.
// Reference validation and blocked-stream limits belong to the decoder.
type SectionPrefix struct{ RequiredInsertCount, Base uint64 }

// ConsumeSectionPrefix uses the negotiated maximum table capacity, never the
// current capacity, to reconstruct the modulo-encoded insert count. Errors
// consume no input. Values and reconstructed counts are limited to 62 bits.
func ConsumeSectionPrefix(src []byte, maxCapacity, totalInserts uint64) (SectionPrefix, int, error) {
	if maxCapacity > MaxInteger || totalInserts > MaxInteger {
		return SectionPrefix{}, 0, ErrIntegerOverflow
	}
	encoded, n, err := ConsumeInteger(src, 8)
	if err != nil {
		return SectionPrefix{}, 0, err
	}
	delta, m, err := ConsumeInteger(src[n:], 7)
	if err != nil {
		return SectionPrefix{}, 0, err
	}
	required, err := reconstructInsertCount(encoded, maxCapacity, totalInserts)
	if err != nil {
		return SectionPrefix{}, 0, err
	}
	base := uint64(0)
	if src[n]&128 != 0 {
		if delta >= required {
			return SectionPrefix{}, 0, ErrSectionPrefix
		}
		base = required - delta - 1
	} else {
		if delta > MaxInteger-required {
			return SectionPrefix{}, 0, ErrIntegerOverflow
		}
		base = required + delta
	}
	return SectionPrefix{required, base}, n + m, nil
}
func reconstructInsertCount(encoded, capacity, total uint64) (uint64, error) {
	if encoded == 0 {
		return 0, nil
	}
	entries := capacity / 32
	full := 2 * entries
	if full == 0 || encoded > full {
		return 0, ErrSectionPrefix
	}
	// The intermediates can exceed 62 bits near the lifetime count limit, but
	// cannot overflow uint64: capacity and total were each bounded to 62 bits.
	ceiling := total + entries
	required := (ceiling/full)*full + encoded - 1
	if required > ceiling {
		if required <= full {
			return 0, ErrSectionPrefix
		}
		required -= full
	}
	if required == 0 {
		return 0, ErrSectionPrefix
	}
	if required > MaxInteger {
		return 0, ErrIntegerOverflow
	}
	return required, nil
}

// AppendSectionPrefix encodes counts without asserting reference availability.
// The caller must ensure references remain within the permitted table window.
func AppendSectionPrefix(dst []byte, p SectionPrefix, maxCapacity uint64) ([]byte, error) {
	if maxCapacity > MaxInteger || p.RequiredInsertCount > MaxInteger || p.Base > MaxInteger {
		return dst, ErrIntegerOverflow
	}
	encoded := uint64(0)
	if p.RequiredInsertCount != 0 {
		full := 2 * (maxCapacity / 32)
		if full == 0 {
			return dst, ErrSectionPrefix
		}
		encoded = p.RequiredInsertCount%full + 1
	}
	flags := byte(0)
	var delta uint64
	if p.Base >= p.RequiredInsertCount {
		delta = p.Base - p.RequiredInsertCount
	} else {
		flags = 128
		delta = p.RequiredInsertCount - p.Base - 1
	}
	dst, _ = AppendInteger(dst, 0, 8, encoded)
	dst, _ = AppendInteger(dst, flags, 7, delta)
	return dst, nil
}
