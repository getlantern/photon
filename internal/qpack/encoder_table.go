package qpack

// EncoderTable coordinates table contents, protected references and encoded
// instructions. Its owner serializes access and reliably sends returned encoder
// instructions in call order. Successful calls commit local state; write failure
// must terminate the connection, not roll back or retry an uncertain write.
// Field selection remains separate; EncodeSection builds and tracks sections.
type EncoderTable struct {
	table *DecoderTable
	refs  *EncoderReferences
}

func NewEncoderTable(maxCapacity uint64, maxBlocked, maxSections, maxRefs int) (*EncoderTable, error) {
	table, err := NewDecoderTable(maxCapacity)
	if err != nil {
		return nil, err
	}
	refs, err := NewEncoderReferences(maxBlocked, maxSections, maxRefs)
	if err != nil {
		return nil, err
	}
	return &EncoderTable{table: table, refs: refs}, nil
}
func (e *EncoderTable) Capacity() uint64                  { return e.table.Capacity() }
func (e *EncoderTable) Size() uint64                      { return e.table.Size() }
func (e *EncoderTable) InsertCount() uint64               { return e.table.InsertCount() }
func (e *EncoderTable) KnownReceivedCount() uint64        { return e.refs.KnownReceivedCount() }
func (e *EncoderTable) Absolute(id uint64) (Field, error) { return e.table.Absolute(id) }
func (e *EncoderTable) Track(streamID uint64, refs []uint64) (uint64, error) {
	return e.refs.Track(streamID, refs)
}
func (e *EncoderTable) ApplyFeedback(v DecoderInstruction) error { return e.refs.ApplyFeedback(v) }

// Apply validates the entire eviction plan and wire instruction before mutation.
// Errors return no wire bytes and preserve both table and reference state.
// Referenced names/duplicates are resolved before potential self-eviction.
func (e *EncoderTable) Apply(v EncoderInstruction) ([]byte, error) {
	var field Field
	var err error
	target := e.table.capacity
	insertion := v.Type != SetDynamicCapacity
	switch v.Type {
	case SetDynamicCapacity:
		if v.Index > e.table.maximum {
			return nil, ErrTableCapacity
		}
		target = v.Index
	case InsertLiteralName:
		field = Field{v.Name, v.Value}
	case InsertNameReference:
		if v.Static {
			field, err = Static(v.Index)
		} else {
			field, err = e.table.Relative(v.Index)
		}
		if err != nil {
			return nil, err
		}
		field.Value = v.Value
	case DuplicateEntry:
		field, err = e.table.Relative(v.Index)
		if err != nil {
			return nil, err
		}
	default:
		return nil, ErrInstruction
	}
	if insertion {
		if e.table.inserts == MaxInteger {
			return nil, ErrIntegerOverflow
		}
		size, err := field.size()
		if err != nil {
			return nil, err
		}
		if size > e.table.capacity {
			return nil, ErrTableCapacity
		}
		target = e.table.capacity - size
	}
	remaining := e.table.size
	dropped := e.refs.dropped
	for offset := e.table.head; remaining > target; offset++ {
		if !e.refs.CanEvict(dropped) {
			return nil, ErrEntryProtected
		}
		size, _ := e.table.entries[offset].size()
		remaining -= size
		dropped++
	}
	wire, err := AppendEncoderInstruction(nil, v)
	if err != nil {
		return nil, err
	}
	// All errors from the table operations are excluded by the checks above.
	if insertion {
		if _, err = e.table.Insert(field); err != nil {
			return nil, err
		}
		e.refs.inserted++
	} else {
		if err = e.table.SetCapacity(v.Index); err != nil {
			return nil, err
		}
	}
	e.refs.dropped = dropped
	return wire, nil
}

// ApplyEarlyLimits promotes accepted 0-RTT limits without replacing table or
// outstanding reference state. A nonzero maximum cannot change (RFC 9204 3.2.3).
// The owner must validate the complete HTTP SETTINGS before calling this method.
func (e *EncoderTable) ApplyEarlyLimits(maxCapacity uint64, maxBlocked int) error {
	if maxCapacity > MaxInteger || (e.table.maximum != 0 && maxCapacity != e.table.maximum) {
		return ErrTableCapacity
	}
	if maxBlocked < e.refs.maxBlocked {
		return ErrBlockedStreams
	}
	e.table.maximum = maxCapacity
	e.refs.maxBlocked = maxBlocked
	return nil
}
