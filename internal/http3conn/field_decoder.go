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
//
// No method writes to the decoder stream or waits for one that does: the peer
// controls that stream's credit, and every request shares this lock. Feedback
// is queued under the lock, in the order the decoder produced it and before the
// result that caused it becomes visible, and one goroutine writes it outside
// the lock while there is any. Feedback only tells the peer's encoder what has
// arrived here, so sending it late costs the peer compression, never
// correctness; its order is what matters. A write failure fails the decoder
// later, through feedbackFailed. The writer must have a deadline or
// cancellation mechanism that interrupts I/O. Any terminal error requires
// connection termination, never retry of uncertain feedback.
type FieldDecoder struct {
	mu       sync.Mutex
	decoder  *qpack.Decoder
	feedback *qpack.FeedbackWriter
	pending  map[uint64]chan DecodedSection
	err      error
	detached error
	// Reports a failure of the decoder stream to the connection owner: a failed
	// write, or a feedback backlog over the limit. A non-nil result means the
	// peer closed gracefully: the feedback is merely undeliverable and the
	// decoder detaches with that cause instead of failing. Runs with the
	// decoder locked.
	feedbackFailed func(error) error

	queue, spare []qpack.DecoderInstruction // unwritten feedback, in order
	maxQueued    int
	flushing     chan struct{} // non-nil while the writer runs; closed when it stops
	// The owner interrupted the decoder stream, which it does only to shut
	// down. Feedback is dropped from then on and the decoder is left for the
	// owner to Close or Detach with the real cause: the interruption surfaces
	// as an expired deadline, which readers would take for a retryable one.
	interrupted bool
}

// defaultFeedbackBacklog bounds unwritten feedback, at 16 bytes an instruction.
// A request owes an acknowledgment per field section and one cancellation, a
// few bytes on the wire in all, so a peer this far behind has stopped reading
// its decoder stream. The bound is far above what the writer's own scheduling
// delay can accumulate.
const defaultFeedbackBacklog = 8192

func NewFieldDecoder(limits qpack.DecoderLimits, writer io.Writer) (*FieldDecoder, error) {
	decoder, err := qpack.NewDecoder(limits)
	if err != nil {
		return nil, err
	}
	feedback, err := qpack.NewFeedbackWriter(writer)
	if err != nil {
		return nil, err
	}
	return &FieldDecoder{decoder: decoder, feedback: feedback, pending: make(map[uint64]chan DecodedSection), maxQueued: defaultFeedbackBacklog}, nil
}

// enqueue schedules feedback for the writer. It runs with the decoder locked
// and never blocks. Adjacent insert count increments are merged, which the
// peer cannot tell from receiving them separately; nothing is reordered,
// because an increment counts from whatever the instructions before it
// acknowledged. A backlog over the limit is a peer that does not read its
// decoder stream, and fails the decoder.
func (d *FieldDecoder) enqueue(instructions []qpack.DecoderInstruction) error {
	if d.detached != nil || d.interrupted {
		return nil
	}
	for _, instruction := range instructions {
		if last := len(d.queue) - 1; instruction.Type == qpack.InsertCountIncrement && last >= 0 && d.queue[last].Type == qpack.InsertCountIncrement {
			d.queue[last].Value += instruction.Value
			continue
		}
		d.queue = append(d.queue, instruction)
	}
	if len(d.queue) > d.maxQueued {
		err := &h3.Error{Code: h3.ExcessiveLoad, Reason: "QPACK decoder feedback backlog"}
		if cause := d.undeliverable(err); cause != nil {
			d.detach(cause)
			return nil
		}
		return d.fail(err)
	}
	if len(d.queue) > 0 && d.flushing == nil {
		d.flushing = make(chan struct{})
		go d.flush(d.flushing)
	}
	return nil
}

// flush writes queued feedback until there is none, or the decoder ends.
func (d *FieldDecoder) flush(done chan struct{}) {
	var batch []qpack.DecoderInstruction
	for {
		d.mu.Lock()
		d.spare = batch[:0]
		if d.err != nil || d.detached != nil || d.interrupted || len(d.queue) == 0 {
			d.queue, d.flushing = d.queue[:0], nil
			d.mu.Unlock()
			close(done)
			return
		}
		batch, d.queue = d.queue, d.spare
		d.mu.Unlock()
		if _, err := d.feedback.Write(batch); err != nil {
			d.mu.Lock()
			if h3.IsDeadline(err) {
				d.interrupted = true
			} else if d.err == nil && d.detached == nil {
				if cause := d.undeliverable(err); cause != nil {
					d.detach(cause)
				} else {
					d.fail(err)
				}
			}
			d.mu.Unlock()
		}
	}
}

// Flushed waits until no feedback is being written. After Close or Detach, and
// once the owner has interrupted the writer, that is when the decoder's
// goroutine has gone.
func (d *FieldDecoder) Flushed() {
	d.mu.Lock()
	flushing := d.flushing
	d.mu.Unlock()
	if flushing != nil {
		<-flushing
	}
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
	if err = d.enqueue(feedback); err != nil {
		return nil, err
	}
	if d.detached != nil && blocked {
		d.decoder.Cancel(id)
		return nil, d.detached
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
	if err = d.enqueue(feedback); err != nil {
		return err
	}
	for _, section := range ready {
		result := d.pending[section.StreamID]
		delete(d.pending, section.StreamID)
		result <- DecodedSection{Fields: section.Fields}
		close(result)
	}
	return d.detached
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
	if err = d.enqueue(feedback); err != nil {
		return err
	}
	if result, ok := d.pending[id]; ok {
		delete(d.pending, id)
		result <- DecodedSection{Err: ErrFieldSectionCanceled}
		close(result)
	}
	return d.detached
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
	d.detach(cause)
}
func (d *FieldDecoder) detach(cause error) {
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

// undeliverable reports a decoder stream that can no longer carry feedback. It
// returns the cause to detach with, or nil when the decoder must fail.
func (d *FieldDecoder) undeliverable(err error) error {
	if d.feedbackFailed == nil {
		return nil
	}
	return d.feedbackFailed(err)
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
