package http3conn

import (
	"errors"
	"io"
	"sync"

	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/qpack"
)

var ErrFieldSectionCanceled = errors.New("http3: field section canceled")

type DecodedSection struct {
	Fields []qpack.DecodedField
	Err    error
}

// FieldDecoder owns receive-side QPACK state and exclusive decoder-stream writes.
// Submit returns a buffered, single-result channel; blocked sections complete
// when EncoderInstruction makes their references available. No per-section
// goroutine is created. Callers preserve section order per stream and consume
// the result before submitting the next section. Methods are concurrency-safe.
// Cancel is called at most once per stream, and the stream owner prevents later
// submissions. Close must be called on connection termination to release waiters.
// The writer must have a deadline or cancellation mechanism that interrupts I/O;
// feedback writes are serialized and must not reenter this owner. Any terminal
// error requires connection termination, never retry of uncertain feedback.
type FieldDecoder struct {
	mu       sync.Mutex
	decoder  *qpack.Decoder
	feedback *qpack.FeedbackWriter
	pending  map[uint64]chan DecodedSection
	err      error
	detached error
	// Reports a request-side feedback write failure, which callers scope to
	// their request, to the connection owner. Runs with the decoder locked.
	feedbackFailed func(error)
}

func NewFieldDecoder(limits qpack.DecoderLimits, writer io.Writer) (*FieldDecoder, error) {
	decoder, err := qpack.NewDecoder(limits)
	if err != nil {
		return nil, err
	}
	feedback, err := qpack.NewFeedbackWriter(writer)
	if err != nil {
		return nil, err
	}
	return &FieldDecoder{decoder: decoder, feedback: feedback, pending: make(map[uint64]chan DecodedSection)}, nil
}
func (s *SettingsState) NewFieldDecoder(limits qpack.DecoderLimits, writer io.Writer) (*FieldDecoder, error) {
	local := s.Local()
	if local.QPACKCapacity > limits.MaxTableCapacity || limits.MaxBlockedStreams < 0 || local.QPACKBlockedStreams > uint64(limits.MaxBlockedStreams) {
		return nil, qpack.ErrDecoderConfig
	}
	limits.MaxTableCapacity = local.QPACKCapacity
	limits.MaxBlockedStreams = int(local.QPACKBlockedStreams)
	limits.MaxDecodedSection = min(limits.MaxDecodedSection, local.MaxFieldSectionSize)
	return NewFieldDecoder(limits, writer)
}
func (d *FieldDecoder) Submit(id uint64, section []byte) (<-chan DecodedSection, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err != nil {
		return nil, d.err
	}
	fields, feedback, blocked, err := d.decoder.Submit(id, section)
	if errors.Is(err, qpack.ErrPendingSection) {
		return nil, err
	}
	if err != nil {
		return nil, d.fail(d.protocolError(err, QPACKDecompressionFailed))
	}
	if d.detached != nil {
		if blocked {
			d.decoder.Cancel(id)
			return nil, d.detached
		}
	} else if _, err = d.feedback.Write(feedback); err != nil {
		return nil, d.failFeedback(err)
	}
	result := make(chan DecodedSection, 1)
	if blocked {
		d.pending[id] = result
	} else {
		result <- DecodedSection{Fields: fields}
		close(result)
	}
	return result, nil
}

// EncoderInstruction accepts exactly one fully framed instruction from Handlers.
// Incomplete or trailing data is a terminal encoder-stream error at this layer.
func (d *FieldDecoder) EncoderInstruction(instruction []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err != nil {
		return d.err
	}
	if d.detached != nil {
		return d.detached
	}
	n, ready, feedback, err := d.decoder.ConsumeEncoder(instruction)
	if err != nil {
		code := QPACKEncoderStreamError
		if n > 0 {
			code = QPACKDecompressionFailed
		}
		return d.fail(d.protocolError(err, code))
	}
	if n != len(instruction) {
		return d.fail(&h3.Error{Code: QPACKEncoderStreamError, Reason: "multiple encoder instructions in one dispatch"})
	}
	if _, err = d.feedback.Write(feedback); err != nil {
		return d.fail(err)
	}
	for _, section := range ready {
		result := d.pending[section.StreamID]
		delete(d.pending, section.StreamID)
		result <- DecodedSection{Fields: section.Fields}
		close(result)
	}
	return nil
}
func (d *FieldDecoder) Cancel(id uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err != nil {
		return d.err
	}
	if d.detached != nil {
		return d.detached
	}
	feedback, err := d.decoder.Cancel(id)
	if err != nil {
		return d.fail(d.protocolError(err, QPACKDecompressionFailed))
	}
	if _, err = d.feedback.Write(feedback); err != nil {
		return d.failFeedback(err)
	}
	if result, ok := d.pending[id]; ok {
		delete(d.pending, id)
		result <- DecodedSection{Err: ErrFieldSectionCanceled}
		close(result)
	}
	return nil
}

// Detach ends the instruction and feedback streams after a graceful peer close
// without discarding table state: sections already buffered on accepted streams
// still decode, and their undeliverable feedback is dropped. Sections that
// remain blocked fail with cause, as do Cancel and EncoderInstruction.
func (d *FieldDecoder) Detach(cause error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err != nil || d.detached != nil {
		return
	}
	if cause == nil {
		cause = io.ErrClosedPipe
	}
	d.detached = cause
	for id, result := range d.pending {
		d.decoder.Cancel(id)
		result <- DecodedSection{Err: cause}
		close(result)
		delete(d.pending, id)
	}
}
func (d *FieldDecoder) Close(cause error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err != nil {
		return
	}
	if cause == nil {
		cause = io.ErrClosedPipe
	}
	d.fail(cause)
}
func (d *FieldDecoder) protocolError(err error, code uint64) error {
	if errors.Is(err, qpack.ErrSectionLimit) {
		code = h3.ExcessiveLoad
	}
	return &h3.Error{Code: code, Reason: err.Error()}
}
func (d *FieldDecoder) failFeedback(err error) error {
	if d.feedbackFailed != nil {
		d.feedbackFailed(err)
	}
	return d.fail(err)
}
func (d *FieldDecoder) fail(err error) error {
	d.err = err
	for id, result := range d.pending {
		result <- DecodedSection{Err: err}
		close(result)
		delete(d.pending, id)
	}
	d.decoder = nil
	return err
}
