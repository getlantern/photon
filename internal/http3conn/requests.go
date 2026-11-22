package http3conn

import (
	"context"
	"errors"

	"github.com/getlantern/photon/internal/wire"
)

var (
	ErrRequestLimit    = errors.New("http3: managed request limit reached")
	ErrRequestID       = errors.New("http3: invalid or already active request ID")
	ErrPeerDraining    = errors.New("http3: peer GOAWAY prohibits new requests")
	ErrRequestRejected = errors.New("http3: peer GOAWAY rejected request")
)

// RequestLease tracks one client-initiated request until its owner calls Finish.
// Its context is canceled when the session terminates or a server GOAWAY rejects
// this ID. ErrRequestRejected means GOAWAY confirms the peer did not process this
// request; generic connection failure does not make that assurance. The owner
// must interrupt transport I/O, cancel QPACK work and apply its retry policy.
type RequestLease struct {
	session *Session
	id      uint64
	ctx     context.Context
	cancel  context.CancelCauseFunc
}

func (r *RequestLease) Context() context.Context { return r.ctx }

// Finish releases the budget and context. It is idempotent and concurrency-safe.
func (r *RequestLease) Finish() {
	s := r.session
	s.requestsMu.Lock()
	defer s.requestsMu.Unlock()
	if s.requests[r.id] == r {
		delete(s.requests, r.id)
	}
	r.cancel(context.Canceled)
}

// BeginRequest commits request admission atomically with received GOAWAY updates.
// It is client-only; MaxRequests=0 disables managed admission. The endpoint calls
// it exactly once per allocated request stream, before transmitting the request,
// and must never reuse an ID (no lifetime tombstone map is retained). If admission
// fails, the endpoint must release/reset the unused transport stream. A lease
// rejected later remains charged until Finish, bounding retained request state.
func (s *Session) BeginRequest(id uint64) (*RequestLease, error) {
	s.requestsMu.Lock()
	defer s.requestsMu.Unlock()
	if s.ctx.Err() != nil {
		return nil, context.Cause(s.ctx)
	}
	if s.server || id > wire.MaxVarint || id%4 != 0 {
		return nil, ErrRequestID
	}
	if s.goaway.Load() != nil {
		return nil, ErrPeerDraining
	}
	if s.requests[id] != nil {
		return nil, ErrRequestID
	}
	if len(s.requests) >= s.maxRequests {
		return nil, ErrRequestLimit
	}
	ctx, cancel := context.WithCancelCause(s.ctx)
	r := &RequestLease{session: s, id: id, ctx: ctx, cancel: cancel}
	if s.requests == nil {
		s.requests = make(map[uint64]*RequestLease)
	}
	s.requests[id] = r
	return r, nil
}

func (s *Session) receiveGoaway(state PeerGoaway) {
	s.requestsMu.Lock()
	defer s.requestsMu.Unlock()
	if s.goaway.Load() == nil && s.goawayReady != nil {
		close(s.goawayReady)
	}
	s.goaway.Store(&state)
	if state.RequestIDs {
		for id, request := range s.requests {
			if state.Rejects(id) {
				request.cancel(ErrRequestRejected)
			}
		}
	}
}

// GoawayReceived closes on the first peer GOAWAY, so client-side stream allocation
// and queued requests can stop without canceling lower-ID active requests.
func (s *Session) GoawayReceived() <-chan struct{} {
	s.requestsMu.Lock()
	defer s.requestsMu.Unlock()
	if s.goawayReady == nil {
		s.goawayReady = make(chan struct{})
		if s.goaway.Load() != nil {
			close(s.goawayReady)
		}
	}
	return s.goawayReady
}
