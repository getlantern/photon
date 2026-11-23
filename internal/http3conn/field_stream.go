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
// the request owner's responsibility. Nothing here waits for the decoder
// stream: the shared decoder queues its feedback. The one wait is for a
// dynamic-table insertion the section references, which the peer controls and
// which ends with Decode's context, the read deadline, or Cancel.
type FieldStream struct {
	decoder        *FieldDecoder
	id             uint64
	mu             sync.Mutex
	busy, canceled bool
	cancelErr      error
	pending        <-chan DecodedSection // section a read deadline left undecided
	deadline       time.Time
	changed        chan struct{} // closed when deadline changes
}

func NewFieldStream(decoder *FieldDecoder, id uint64) (*FieldStream, error) {
	if decoder == nil || id > qpack.MaxInteger {
		return nil, fmt.Errorf("http3: invalid field stream")
	}
	return &FieldStream{decoder: decoder, id: id, changed: make(chan struct{})}, nil
}

// SetReadDeadline bounds Decode's wait, including one already in progress. An
// expired deadline returns os.ErrDeadlineExceeded and keeps the section: the
// next Decode resumes waiting for it and ignores its argument.
func (s *FieldStream) SetReadDeadline(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deadline = t
	close(s.changed)
	s.changed = make(chan struct{})
}
func (s *FieldStream) Decode(ctx context.Context, section []byte) ([]qpack.DecodedField, error) {
	if err := ctx.Err(); err != nil {
		if cancelErr := s.Cancel(); cancelErr != nil {
			return nil, cancelErr
		}
		return nil, err
	}
	s.mu.Lock()
	if s.canceled {
		err := s.cancelErr
		s.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return nil, ErrFieldSectionCanceled
	}
	if s.busy {
		s.mu.Unlock()
		return nil, qpack.ErrPendingSection
	}
	result := s.pending
	if result == nil {
		submitted, err := s.decoder.Submit(s.id, section)
		if err != nil {
			s.mu.Unlock()
			return nil, err
		}
		result = submitted
	}
	s.pending, s.busy = nil, true
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.busy = false; s.mu.Unlock() }()
	// One timer for the whole wait: the peer decides how long it lasts, and
	// the deadline may change any number of times meanwhile.
	timer := time.NewTimer(0)
	timer.Stop()
	defer timer.Stop()
	for {
		// A section that is ready wins over a deadline that has passed.
		select {
		case decoded := <-result:
			return decoded.Fields, decoded.Err
		default:
		}
		s.mu.Lock()
		deadline, changed := s.deadline, s.changed
		s.mu.Unlock()
		var expired <-chan time.Time
		timer.Stop()
		if !deadline.IsZero() {
			timer.Reset(time.Until(deadline))
			expired = timer.C
		}
		select {
		case decoded := <-result:
			return decoded.Fields, decoded.Err
		case <-ctx.Done():
			if err := s.Cancel(); err != nil {
				return nil, err
			}
			return nil, ctx.Err()
		case <-expired:
			s.mu.Lock()
			s.pending = result
			s.mu.Unlock()
			return nil, os.ErrDeadlineExceeded
		case <-changed:
		}
	}
}

// Cancel emits at most one cancellation instruction even when called repeatedly
// or concurrently. No subsequent Decode can submit to the shared decoder, and
// one that is waiting returns ErrFieldSectionCanceled.
func (s *FieldStream) Cancel() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.canceled {
		return s.cancelErr
	}
	s.canceled = true
	s.cancelErr = s.decoder.Cancel(s.id)
	return s.cancelErr
}
