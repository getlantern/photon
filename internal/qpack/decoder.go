package qpack

import (
	"bytes"
	"errors"
	"io"
	"sort"
)

var (
	ErrBlockedStreams = errors.New("qpack: blocked stream limit exceeded")
	ErrPendingSection = errors.New("qpack: stream already has a pending section")
	ErrDecoderConfig  = errors.New("qpack: invalid decoder limits")
)

type DecoderLimits struct {
	MaxTableCapacity           uint64
	MaxBlockedStreams          int
	MaxBufferedBytes           int
	MaxEncodedSection          int
	MaxDecodedSection          uint64
	MaxFields                  int
	MaxInstructionLiteralBytes int
}
type pendingSection struct {
	prefix  SectionPrefix
	body    []byte
	charged int
}
type SectionResult struct {
	StreamID uint64
	Fields   []DecodedField
}

// Decoder owns receive-side QPACK state. Its caller serializes all operations,
// submits sections in stream order, and stops reading further sections on a
// stream while one is pending. Returned instructions must be sent reliably in
// call order; they are already accounted for and cannot be discarded/reordered.
// No lifetime stream-ID tombstones are retained: the HTTP owner must prevent
// submission after cancellation and issue cancellation at most once per stream.
type Decoder struct {
	limits   DecoderLimits
	table    *DecoderTable
	pending  map[uint64]pendingSection
	buffered int
	reported uint64
	err      error
}

func NewDecoder(limits DecoderLimits) (*Decoder, error) {
	if limits.MaxTableCapacity > MaxInteger || limits.MaxDecodedSection > MaxInteger || limits.MaxBlockedStreams < 0 || limits.MaxBufferedBytes < 0 || limits.MaxEncodedSection < 0 || limits.MaxFields < 0 || limits.MaxInstructionLiteralBytes < 0 {
		return nil, ErrDecoderConfig
	}
	table, _ := NewDecoderTable(limits.MaxTableCapacity)
	return &Decoder{limits: limits, table: table, pending: make(map[uint64]pendingSection)}, nil
}
func (d *Decoder) BlockedStreams() int { return len(d.pending) }
func (d *Decoder) BufferedBytes() int  { return d.buffered }
func (d *Decoder) fail(err error) error {
	d.err = err
	clear(d.pending)
	d.buffered = 0
	return err
}
func (d *Decoder) acknowledge(id, required uint64) []DecoderInstruction {
	if required == 0 {
		return nil
	}
	if required > d.reported {
		d.reported = required
	}
	return []DecoderInstruction{{Type: SectionAcknowledgment, Value: id}}
}

// Submit returns blocked=true only after taking an owned copy of the section.
// ErrPendingSection is local backpressure and leaves the decoder usable. Other
// failures are terminal; the endpoint must map their causes to HTTP/3 errors.
func (d *Decoder) Submit(id uint64, src []byte) (fields []DecodedField, instructions []DecoderInstruction, blocked bool, err error) {
	if d.err != nil {
		return nil, nil, false, d.err
	}
	if id > MaxInteger {
		return nil, nil, false, d.fail(ErrIntegerOverflow)
	}
	if _, ok := d.pending[id]; ok {
		return nil, nil, false, ErrPendingSection
	}
	if len(src) > d.limits.MaxEncodedSection {
		return nil, nil, false, d.fail(ErrSectionLimit)
	}
	p, n, err := ConsumeSectionPrefix(src, d.table.maximum, d.table.inserts)
	if err != nil {
		return nil, nil, false, d.fail(err)
	}
	if p.RequiredInsertCount > d.table.inserts {
		if len(d.pending) >= d.limits.MaxBlockedStreams {
			return nil, nil, false, d.fail(ErrBlockedStreams)
		}
		if len(src) > d.limits.MaxBufferedBytes-d.buffered {
			return nil, nil, false, d.fail(ErrSectionLimit)
		}
		d.pending[id] = pendingSection{prefix: p, body: bytes.Clone(src[n:]), charged: len(src)}
		d.buffered += len(src)
		return nil, nil, true, nil
	}
	fields, err = d.table.DecodeSectionFields(src[n:], p, d.limits.MaxDecodedSection, d.limits.MaxFields)
	if err != nil {
		return nil, nil, false, d.fail(err)
	}
	return fields, d.acknowledge(id, p.RequiredInsertCount), false, nil
}

// ConsumeEncoder consumes one complete instruction, then resumes all newly
// ready sections before another insertion can evict their entries. Incomplete
// input consumes nothing and is retryable. All other errors are terminal.
// Framing/buffering the encoder stream and critical-stream closure are external.
func (d *Decoder) ConsumeEncoder(src []byte) (n int, ready []SectionResult, instructions []DecoderInstruction, err error) {
	if d.err != nil {
		return 0, nil, nil, d.err
	}
	n, err = d.table.ConsumeEncoderUpdate(src, d.limits.MaxInstructionLiteralBytes)
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, nil, nil, err
		}
		return 0, nil, nil, d.fail(err)
	}
	var ids []uint64
	for id, p := range d.pending {
		if p.prefix.RequiredInsertCount <= d.table.inserts {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		p := d.pending[id]
		fields, e := d.table.DecodeSectionFields(p.body, p.prefix, d.limits.MaxDecodedSection, d.limits.MaxFields)
		if e != nil {
			return n, nil, nil, d.fail(e)
		}
		ready = append(ready, SectionResult{StreamID: id, Fields: fields})
		instructions = append(instructions, d.acknowledge(id, p.prefix.RequiredInsertCount)...)
		d.buffered -= p.charged
		delete(d.pending, id)
	}
	// A section acknowledgment implicitly acknowledges insertions through its RIC.
	// Explicit increments cover remaining speculative insertions, never zero.
	if d.table.inserts > d.reported {
		instructions = append(instructions, DecoderInstruction{Type: InsertCountIncrement, Value: d.table.inserts - d.reported})
		d.reported = d.table.inserts
	}
	return n, ready, instructions, nil
}

// Cancel abandons all pending work for a stream. It also emits cancellation
// without a queued section, covering resets before HEADERS arrive. This does
// not acknowledge insertions. The owner calls it at most once per stream.
func (d *Decoder) Cancel(id uint64) ([]DecoderInstruction, error) {
	if d.err != nil {
		return nil, d.err
	}
	if id > MaxInteger {
		return nil, d.fail(ErrIntegerOverflow)
	}
	if p, ok := d.pending[id]; ok {
		d.buffered -= p.charged
		delete(d.pending, id)
	}
	return []DecoderInstruction{{Type: StreamCancellation, Value: id}}, nil
}
