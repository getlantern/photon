package qpack

// EncodeSection encodes chosen field representations and commits their dynamic
// references only after all validation/encoding succeeds. Limits apply to encoded
// bytes, decoded name/value bytes plus 32 per occurrence, and field count.
// Returned bytes must be committed in stream order; write failure is terminal.
// Selection/insertion policy and HTTP semantics are supplied by the caller.
func (e *EncoderTable) EncodeSection(streamID, base uint64, lines []FieldLine, maxEncoded int, maxDecoded uint64, maxFields int) ([]byte, error) {
	if streamID > MaxInteger || base > MaxInteger || maxDecoded > MaxInteger {
		return nil, ErrIntegerOverflow
	}
	if maxEncoded < 0 || maxFields < 0 || len(lines) > maxFields {
		return nil, ErrSectionLimit
	}
	var decoded, encoded, required uint64
	var refs []uint64
	for _, v := range lines {
		field, ref, err := e.table.ResolveFieldLine(v, base, e.table.inserts)
		if err != nil {
			return nil, err
		}
		size, err := field.size()
		if err != nil {
			return nil, err
		}
		if size > maxDecoded-decoded {
			return nil, ErrSectionLimit
		}
		decoded += size
		n, err := fieldLineSize(v)
		if err != nil {
			return nil, err
		}
		if n > uint64(maxEncoded)-encoded {
			return nil, ErrSectionLimit
		}
		encoded += n
		if ref > 0 {
			// Bound tracking scratch before storing another reference.
			if len(refs) >= e.refs.maxRefs-e.refs.refs {
				return nil, ErrEncoderLimit
			}
			refs = append(refs, ref-1)
			if ref > required {
				required = ref
			}
		}
	}
	prefix, err := AppendSectionPrefix(nil, SectionPrefix{RequiredInsertCount: required, Base: base}, e.table.maximum)
	if err != nil {
		return nil, err
	}
	if uint64(len(prefix)) > uint64(maxEncoded)-encoded {
		return nil, ErrSectionLimit
	}
	encoded += uint64(len(prefix))
	out := make([]byte, 0, int(encoded))
	out = append(out, prefix...)
	for _, v := range lines {
		out, err = AppendFieldLine(out, v)
		if err != nil {
			return nil, err
		}
	}
	if _, err = e.refs.Track(streamID, refs); err != nil {
		return nil, err
	}
	return out, nil
}
func prefixedSize(value uint64, prefix uint8) uint64 {
	mask := uint64(1<<prefix) - 1
	if value < mask {
		return 1
	}
	value -= mask
	size := uint64(2)
	for value >= 128 {
		size++
		value >>= 7
	}
	return size
}
func encodedStringSize(value string, huffman bool, prefix uint8) (uint64, error) {
	size := uint64(len(value))
	if huffman {
		size = HuffmanSize(value)
	}
	if size > MaxInteger {
		return 0, ErrIntegerOverflow
	}
	return size + prefixedSize(size, prefix), nil
}
func fieldLineSize(v FieldLine) (uint64, error) {
	// Semantic shape and index bounds are checked by ResolveFieldLine first.
	var size uint64
	switch v.Type {
	case IndexedField:
		return prefixedSize(v.Index, 6), nil
	case IndexedPostBase:
		return prefixedSize(v.Index, 4), nil
	case LiteralNameReference:
		size = prefixedSize(v.Index, 4)
	case LiteralPostBase:
		size = prefixedSize(v.Index, 3)
	case LiteralName:
		var err error
		size, err = encodedStringSize(v.Name, v.HuffmanName, 3)
		if err != nil {
			return 0, err
		}
	default:
		return 0, ErrFieldLine
	}
	value, err := encodedStringSize(v.Value, v.HuffmanValue, 7)
	if err != nil {
		return 0, err
	}
	// At most two 62-bit strings and their prefixes fit safely in uint64.
	return size + value, nil
}
