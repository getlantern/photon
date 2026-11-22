package qpack

import "errors"

var (
	ErrSectionBlocked      = errors.New("qpack: field section awaits insertions")
	ErrSectionLimit        = errors.New("qpack: field section exceeds limit")
	ErrRequiredInsertCount = errors.New("qpack: required insert count does not match references")
)

type DecodedField struct {
	Field
	NeverIndex bool
}

// DecodeSection parses one complete field-section payload. Size accounting is
// name bytes + value bytes + 32 per occurrence, including indexed/duplicate fields.
// maxFields independently bounds result storage. No partial fields escape errors.
// On ErrSectionBlocked, the returned prefix is resolved at receipt time. Retain
// it and the remaining payload (after ConsumeSectionPrefix's byte count), then
// resume with DecodeSectionFields; do not reconstruct a modulo count again after
// subsequent insertions. The stream owner enforces blocked-stream/buffer limits.
func (t *DecoderTable) DecodeSection(src []byte, maxSize uint64, maxFields int) (SectionPrefix, []DecodedField, error) {
	p, n, err := ConsumeSectionPrefix(src, t.maximum, t.inserts)
	if err != nil {
		return SectionPrefix{}, nil, err
	}
	fields, err := t.DecodeSectionFields(src[n:], p, maxSize, maxFields)
	return p, fields, err
}

// DecodeSectionFields decodes field lines using an already reconstructed prefix.
// The owner must serialize this operation with table updates. HTTP name/value
// semantics and acknowledgment scheduling belong to the endpoint.
func (t *DecoderTable) DecodeSectionFields(src []byte, p SectionPrefix, maxSize uint64, maxFields int) ([]DecodedField, error) {
	if maxSize > MaxInteger || p.Base > MaxInteger || p.RequiredInsertCount > MaxInteger {
		return nil, ErrIntegerOverflow
	}
	if maxFields < 0 {
		return nil, ErrSectionLimit
	}
	if p.RequiredInsertCount > t.inserts {
		return nil, ErrSectionBlocked
	}
	var fields []DecodedField
	var size, highest uint64
	for len(src) > 0 {
		if len(fields) == maxFields || maxSize-size < 32 {
			return nil, ErrSectionLimit
		}
		literalBudget := maxSize - size - 32
		// Safe on both 32-bit and 64-bit hosts.
		if literalBudget > uint64(int(^uint(0)>>1)) {
			literalBudget = uint64(int(^uint(0) >> 1))
		}
		v, n, err := ConsumeFieldLine(src, int(literalBudget))
		if errors.Is(err, ErrStringLimit) {
			return nil, ErrSectionLimit
		}
		if err != nil {
			return nil, err
		}
		field, ref, err := t.ResolveFieldLine(v, p.Base, p.RequiredInsertCount)
		if err != nil {
			return nil, err
		}
		fieldSize, err := field.size()
		if err != nil {
			return nil, err
		}
		if fieldSize > maxSize-size {
			return nil, ErrSectionLimit
		}
		size += fieldSize
		if ref > highest {
			highest = ref
		}
		fields = append(fields, DecodedField{Field: field, NeverIndex: v.NeverIndex})
		src = src[n:]
	}
	// RFC 9204 section 2.2.3 permits rejecting an unnecessarily large count.
	if highest != p.RequiredInsertCount {
		return nil, ErrRequiredInsertCount
	}
	return fields, nil
}
