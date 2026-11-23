package http3conn

import (
	"fmt"
	"sync"

	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/wire"
)

// RequestPriorities serializes request-header and control-stream signals. The
// request accept loop must register streams in increasing ID order. Both active
// and not-yet-accepted state are bounded; completed requests require no tombstone
// allocation because their IDs are below the acceptance watermark.
type RequestPriorities struct {
	mu      sync.Mutex
	limit   int
	nextID  uint64
	active  map[uint64]*requestPriorityState
	pending map[uint64]h3.Priority
}
type requestPriorityState struct {
	stream  ServerRequestStream
	updated bool
}

func NewRequestPriorities(limit int) (*RequestPriorities, error) {
	if limit < 1 {
		return nil, fmt.Errorf("http3: invalid priority state limit")
	}
	return &RequestPriorities{limit: limit, active: make(map[uint64]*requestPriorityState), pending: make(map[uint64]h3.Priority)}, nil
}

// Update accepts only request events from the validated control reader. Invalid
// dictionaries leave the latest valid signal intact. A complete update replaces
// previous parameters, including defaults for parameters omitted by the peer.
func (p *RequestPriorities) Update(event h3.ControlEvent) error {
	if event.Type != h3.PriorityUpdateRequest || !event.PriorityValid {
		return nil
	}
	if event.ID > wire.MaxVarint || event.ID%4 != 0 {
		return &h3.Error{Code: h3.IDError, Reason: "priority target is not a request stream"}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if state := p.active[event.ID]; state != nil {
		state.updated = true
		applyRequestPriority(state.stream, event.Priority)
		return nil
	}
	if event.ID < p.nextID {
		return nil
	}
	if _, exists := p.pending[event.ID]; !exists && len(p.pending) >= p.limit {
		return &h3.Error{Code: h3.ExcessiveLoad, Reason: "pending priority state limit"}
	}
	p.pending[event.ID] = event.Priority
	return nil
}

func (p *RequestPriorities) register(stream ServerRequestStream) error {
	id := uint64(stream.StreamID())
	p.mu.Lock()
	defer p.mu.Unlock()
	if id%4 != 0 || id > wire.MaxVarint || id < p.nextID {
		return fmt.Errorf("http3: priority registration requires increasing request IDs")
	}
	if len(p.active) >= p.limit {
		return &h3.Error{Code: h3.ExcessiveLoad, Reason: "active priority state limit"}
	}
	state := &requestPriorityState{stream: stream}
	if priority, ok := p.pending[id]; ok {
		state.updated = true
		applyRequestPriority(stream, priority)
		delete(p.pending, id)
	}
	p.active[id] = state
	p.nextID = id + 4
	// Generic acceptors may skip IDs. No skipped ID can subsequently be accepted.
	for pending := range p.pending {
		if pending < p.nextID {
			delete(p.pending, pending)
		}
	}
	return nil
}

func (p *RequestPriorities) header(stream ServerRequestStream, priority h3.Priority) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if state := p.active[uint64(stream.StreamID())]; state != nil && !state.updated {
		applyRequestPriority(state.stream, priority)
	}
}
func (p *RequestPriorities) finish(id uint64) { p.mu.Lock(); delete(p.active, id); p.mu.Unlock() }
func applyRequestPriority(stream ServerRequestStream, priority h3.Priority) {
	if scheduler, ok := stream.(interface{ SetPriority(int8, bool) }); ok {
		scheduler.SetPriority(priority.Urgency, priority.Incremental)
	}
}
