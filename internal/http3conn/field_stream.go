package http3conn

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/getlantern/photon/internal/qpack"
)

// FieldStream coordinates one request stream's section decoding and cancellation.
// Create exactly one per stream. Decode calls must be sequential; Cancel may run
// concurrently (for example, after a request reset). A canceled Decode context
// cancels this entire stream's QPACK work. Transport reset/STOP_SENDING remains
// the request owner's responsibility. The shared decoder writer must remain
// interruptible: cancellation feedback can itself block on transport I/O.
//
// The shared decoder serializes every stream behind one lock and writes
// feedback under it, to a stream whose credit the peer controls. The mutex here
// is therefore never held across a decoder call: SetReadDeadline and
// StopReading must not wait for the peer.
type FieldStream struct {
	decoder    *FieldDecoder
	id         uint64
	mu         sync.Mutex
	busy       bool
	canceled   bool // Cancel has started; nothing more is submitted
	cancelOnce sync.Once
	cancelErr  error
	flight     *submission // submitted and not yet returned by Decode
	// A raw stream's receive controls reach Decode itself: while it waits for
	// the decoder or the dynamic table, nothing arrives on the request stream
	// to carry them there.
	detached bool // set by Detach
	deadline time.Time
	changed  chan struct{} // closed when deadline changes
	stopped  chan struct{} // closed by StopReading
	stopErr  error
}

// submission is one section handed to the shared decoder. done closes when the
// decoder has taken it, which can wait for feedback credit; result then yields
// the fields, which can wait for the dynamic table.
type submission struct {
	done   chan struct{}
	result <-chan DecodedSection
	err    error
}

func NewFieldStream(decoder *FieldDecoder, id uint64) (*FieldStream, error) {
	if decoder == nil || id > qpack.MaxInteger {
		return nil, fmt.Errorf("http3: invalid field stream")
	}
	return &FieldStream{decoder: decoder, id: id, changed: make(chan struct{}), stopped: make(chan struct{})}, nil
}

// Detach lets Decode return while the shared decoder still holds its section:
// on StopReading, on an expired read deadline, and when its context ends. The
// section is then submitted from a goroutine, and a context that ends no
// longer writes the QPACK cancellation from Decode; the owner's Cancel does.
// Call it before the Decode calls it should apply to.
func (s *FieldStream) Detach() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.detached = true
}

// SetReadDeadline bounds Decode on a detached stream, including a call already
// in progress. An expired deadline returns os.ErrDeadlineExceeded and keeps the
// section: the next Decode resumes waiting for it and ignores its argument.
func (s *FieldStream) SetReadDeadline(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deadline = t
	close(s.changed)
	s.changed = make(chan struct{})
}

// StopReading makes the current and every later Decode return err, without
// writing to the decoder stream. The owner still calls Cancel on cleanup.
func (s *FieldStream) StopReading(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopErr == nil {
		s.stopErr = err
		close(s.stopped)
	}
}
func (s *FieldStream) Decode(ctx context.Context, section []byte) ([]qpack.DecodedField, error) {
	s.mu.Lock()
	detached := s.detached
	s.mu.Unlock()
	// abandon ends this stream's decoding for a context that is done.
	abandon := func() error {
		if detached {
			s.StopReading(ctx.Err())
			return ctx.Err()
		}
		if err := s.Cancel(); err != nil {
			return err
		}
		return ctx.Err()
	}
	if ctx.Err() != nil {
		return nil, abandon()
	}
	s.mu.Lock()
	if s.canceled {
		s.mu.Unlock()
		if err := s.Cancel(); err != nil {
			return nil, err
		}
		return nil, ErrFieldSectionCanceled
	}
	if s.stopErr != nil {
		err := s.stopErr
		s.mu.Unlock()
		return nil, err
	}
	if s.busy {
		s.mu.Unlock()
		return nil, qpack.ErrPendingSection
	}
	flight, submit := s.flight, func() {}
	if flight == nil {
		flight = &submission{done: make(chan struct{})}
		s.flight = flight
		submit = func() {
			flight.result, flight.err = s.decoder.Submit(s.id, section)
			close(flight.done)
		}
	}
	s.busy = true
	s.mu.Unlock()
	finish := func() { s.mu.Lock(); s.busy, s.flight = false, nil; s.mu.Unlock() }
	if detached {
		go submit()
	} else {
		submit()
	}
	taken := flight.done
	var result <-chan DecodedSection
	for {
		// A section that is ready wins over a deadline that has passed.
		select {
		case <-taken:
			if flight.err != nil {
				finish()
				return nil, flight.err
			}
			taken, result = nil, flight.result
			continue
		case decoded := <-result:
			finish()
			return decoded.Fields, decoded.Err
		default:
		}
		s.mu.Lock()
		deadline, changed := s.deadline, s.changed
		s.mu.Unlock()
		var expired <-chan time.Time
		if detached && !deadline.IsZero() {
			timer := time.NewTimer(time.Until(deadline))
			defer timer.Stop()
			expired = timer.C
		}
		select {
		case <-taken:
		case decoded := <-result:
			finish()
			return decoded.Fields, decoded.Err
		case <-ctx.Done():
			// The section stays in flight for Cancel to settle.
			s.mu.Lock()
			s.busy = false
			s.mu.Unlock()
			return nil, abandon()
		case <-s.stopped:
			s.mu.Lock()
			s.busy = false
			s.mu.Unlock()
			return nil, s.stopErr
		case <-expired:
			s.mu.Lock()
			s.busy = false
			s.mu.Unlock()
			return nil, os.ErrDeadlineExceeded
		case <-changed:
		}
	}
}

// Cancel emits at most one cancellation instruction even when called repeatedly
// or concurrently. No subsequent Decode can submit to the shared decoder. It
// waits for a section the decoder is still taking.
func (s *FieldStream) Cancel() error {
	s.mu.Lock()
	s.canceled = true
	flight := s.flight
	s.mu.Unlock()
	s.cancelOnce.Do(func() {
		if flight != nil {
			<-flight.done
		}
		s.cancelErr = s.decoder.Cancel(s.id)
	})
	return s.cancelErr
}
