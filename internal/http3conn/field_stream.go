package http3conn

import (
	"context"
	"fmt"
	"sync"

	"github.com/getlantern/photon/internal/qpack"
)

// FieldStream coordinates one request stream's section decoding and cancellation.
// Create exactly one per stream. Decode calls must be sequential; Cancel may run
// concurrently (for example, after a request reset). A canceled Decode context
// cancels this entire stream's QPACK work. Transport reset/STOP_SENDING remains
// the request owner's responsibility. The shared decoder writer must remain
// interruptible: cancellation feedback can itself block on transport I/O.
type FieldStream struct {
	decoder        *FieldDecoder
	id             uint64
	mu             sync.Mutex
	busy, canceled bool
	cancelErr      error
}

func NewFieldStream(decoder *FieldDecoder, id uint64) (*FieldStream, error) {
	if decoder == nil || id > qpack.MaxInteger {
		return nil, fmt.Errorf("http3: invalid field stream")
	}
	return &FieldStream{decoder: decoder, id: id}, nil
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
	result, err := s.decoder.Submit(s.id, section)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	s.busy = true
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.busy = false; s.mu.Unlock() }()
	select {
	case decoded := <-result:
		return decoded.Fields, decoded.Err
	case <-ctx.Done():
		if err := s.Cancel(); err != nil {
			return nil, err
		}
		return nil, ctx.Err()
	}
}

// Cancel emits at most one cancellation instruction even when called repeatedly
// or concurrently. No subsequent Decode can submit to the shared decoder.
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
