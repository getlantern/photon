package qpack

import "errors"

// InsertField applies an opt-in eager insertion policy for one field. It skips
// never-index fields, exact static/dynamic matches, oversized entries, protected
// eviction victims, and instructions exceeding maxEncoded. A skipped insertion
// returns (nil,false,nil) and leaves state unchanged; callers can use literals.
// It chooses the smallest insertion encoding, using Huffman only when smaller.
// This policy has no admission history and makes no compression/performance
// optimality claim. Capacity must be configured separately with Apply.
//
// Returned bytes are committed table updates: the owner must send them reliably
// on the encoder stream even if a later EncodeFields operation fails. The two
// operations are deliberately separate commits, not a transaction across streams.
func (e *EncoderTable) InsertField(field DecodedField, maxEncoded int) ([]byte, bool, error) {
	if maxEncoded < 0 {
		return nil, false, ErrEncoderLimit
	}
	if field.NeverIndex {
		return nil, false, nil
	}
	size, err := field.Field.size()
	if err != nil {
		return nil, false, err
	}
	if size > e.table.capacity {
		return nil, false, nil
	}
	staticIndex, exact, found := FindStatic(field.Field)
	if exact {
		return nil, false, nil
	}
	valueHuffman := HuffmanSize(field.Value) < uint64(len(field.Value))
	best := EncoderInstruction{Type: InsertLiteralName, Name: field.Name, Value: field.Value, HuffmanName: HuffmanSize(field.Name) < uint64(len(field.Name)), HuffmanValue: valueHuffman}
	nameSize, err := encodedStringSize(field.Name, best.HuffmanName, 5)
	if err != nil {
		return nil, false, err
	}
	valueSize, err := encodedStringSize(field.Value, valueHuffman, 7)
	if err != nil {
		return nil, false, err
	}
	bestSize := nameSize + valueSize
	consider := func(index uint64, static bool) {
		size := prefixedSize(index, 6) + valueSize
		if size < bestSize {
			best = EncoderInstruction{Type: InsertNameReference, Index: index, Static: static, Value: field.Value, HuffmanValue: valueHuffman}
			bestSize = size
		}
	}
	if found {
		consider(staticIndex, true)
	}
	for id := e.table.inserts; id > e.refs.dropped; {
		id--
		entry, _ := e.table.Absolute(id)
		if entry == field.Field {
			return nil, false, nil
		}
		if entry.Name == field.Name {
			consider(e.table.inserts-id-1, false)
		}
	}
	if bestSize > uint64(maxEncoded) {
		return nil, false, nil
	}
	wire, err := e.Apply(best)
	if errors.Is(err, ErrEntryProtected) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return wire, true, nil
}
