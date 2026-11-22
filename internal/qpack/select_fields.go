package qpack

// EncodeFields chooses representations for ordered fields, retaining duplicates
// and NeverIndex flags. It uses the shortest field-line form among available
// static entries, permitted dynamic entries, and literals. Huffman coding is
// used only when it shortens a string. Whole-section global optimality is not
// claimed: dynamic-prefix overhead and future compression are not optimized.
// Insertion policy remains explicit through InsertField or Apply; this method never inserts.
func (e *EncoderTable) EncodeFields(streamID uint64, fields []DecodedField, maxEncoded int, maxDecoded uint64, maxFields int) ([]byte, error) {
	if streamID > MaxInteger || maxDecoded > MaxInteger {
		return nil, ErrIntegerOverflow
	}
	if maxEncoded < 0 || maxFields < 0 || len(fields) > maxFields {
		return nil, ErrSectionLimit
	}
	var total uint64
	for _, f := range fields {
		size, err := f.Field.size()
		if err != nil {
			return nil, err
		}
		if size > maxDecoded-total {
			return nil, ErrSectionLimit
		}
		total += size
	}
	lines := make([]FieldLine, 0, len(fields))
	base := e.table.inserts
	allowBlocking := e.refs.streamBlocked(streamID) || e.refs.BlockedStreams() < e.refs.maxBlocked
	refBudget := e.refs.maxRefs - e.refs.refs
	if e.refs.sections >= e.refs.maxSections {
		refBudget = 0
	}
	used := 0
	for _, f := range fields {
		valueHuffman := HuffmanSize(f.Value) < uint64(len(f.Value))
		best := FieldLine{Type: LiteralName, Name: f.Name, Value: f.Value, NeverIndex: f.NeverIndex, HuffmanName: HuffmanSize(f.Name) < uint64(len(f.Name)), HuffmanValue: valueHuffman}
		bestSize, err := fieldLineSize(best)
		if err != nil {
			return nil, err
		}
		dynamic := false
		consider := func(v FieldLine, isDynamic bool) {
			size, err := fieldLineSize(v)
			if err == nil && size < bestSize {
				best, bestSize, dynamic = v, size, isDynamic
			}
		}
		if index, exact, found := FindStatic(f.Field); found {
			if exact && !f.NeverIndex {
				consider(FieldLine{Type: IndexedField, Static: true, Index: index}, false)
			}
			consider(FieldLine{Type: LiteralNameReference, Static: true, Index: staticNames[f.Name], Value: f.Value, NeverIndex: f.NeverIndex, HuffmanValue: valueHuffman}, false)
		}
		if used < refBudget {
			// Newest-first makes lower relative indices win ties. This bounded linear
			// scan can be replaced by a lookup index once measurements justify it.
			for id := base; id > e.refs.dropped; {
				id--
				if id >= e.refs.known && !allowBlocking {
					continue
				}
				entry, _ := e.table.Absolute(id)
				if entry.Name != f.Name {
					continue
				}
				if entry.Value == f.Value && !f.NeverIndex {
					consider(FieldLine{Type: IndexedField, Index: base - id - 1}, true)
				}
				consider(FieldLine{Type: LiteralNameReference, Index: base - id - 1, Value: f.Value, NeverIndex: f.NeverIndex, HuffmanValue: valueHuffman}, true)
			}
		}
		if dynamic {
			used++
		}
		lines = append(lines, best)
	}
	if used == 0 {
		base = 0
	}
	return e.EncodeSection(streamID, base, lines, maxEncoded, maxDecoded, maxFields)
}
