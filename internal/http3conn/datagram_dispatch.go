package http3conn

import (
	"context"
	"errors"
	"sync"

	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/wire"
)

var ErrNoHTTPDatagram = errors.New("http3: no queued datagram")
var ErrDatagramRequestClosed = errors.New("http3: datagram request receive side closed")

// DatagramRequestError requires aborting this request, not the connection.
// An extension can override the default unsupported-request policy by registering
// its semantics before dispatch. Unknown and closed request IDs are dropped.
type DatagramRequestError struct{ ID uint64 }

func (e *DatagramRequestError) Error() string {
	return "http3: datagram on request without datagram semantics"
}
func (e *DatagramRequestError) Code() uint64 { return h3.DatagramError }

// DatagramDispatch owns a bounded receive queue per registered request. It does
// not read QUIC, negotiate settings, or create requests on incoming datagrams.
// The endpoint must register every open receive side (including unsupported
// requests), close it on FIN/reset, remove it at final cleanup, and never reuse IDs.
// All methods are safe for concurrent request workers and the receive loop.
type DatagramDispatch struct {
	mu                                 sync.Mutex
	closed                             bool
	requests                           map[uint64]*DatagramRequest
	maxRequests, maxQueued, maxPayload int
}
type DatagramRequest struct {
	pending, pendingSeen bool // guarded by owner.mu
	abortContext         context.Context
	cancelAbort          context.CancelCauseFunc
	abort                func(uint64) // guarded by owner.mu; called outside the lock
	sendGate             chan struct{}
	sendContext          context.Context
	receiveContext       context.Context // guarded by owner.mu
	cancelSend           context.CancelCauseFunc
	sendClosed           bool
	owner                *DatagramDispatch
	id                   uint64
	supported, closed    bool
	queue                [][]byte
	changed              chan struct{}
}

func NewDatagramDispatch(maxRequests, maxQueued, maxPayload int) (*DatagramDispatch, error) {
	if maxRequests < 1 || maxQueued < 1 || maxPayload < 0 {
		return nil, errors.New("http3: invalid datagram limits")
	}
	return &DatagramDispatch{requests: make(map[uint64]*DatagramRequest), maxRequests: maxRequests, maxQueued: maxQueued, maxPayload: maxPayload}, nil
}
func (d *DatagramDispatch) Register(id uint64, supported bool) (*DatagramRequest, error) {
	return d.register(id, supported, false, nil, nil)
}
func (d *DatagramDispatch) register(id uint64, supported, pending bool, receiveContext context.Context, abort func(uint64)) (*DatagramRequest, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, ErrDatagramRequestClosed
	}
	if id > wire.MaxVarint || id%4 != 0 || d.requests[id] != nil {
		return nil, ErrRequestID
	}
	if len(d.requests) >= d.maxRequests {
		return nil, ErrRequestLimit
	}
	r := &DatagramRequest{pending: pending, abort: abort, receiveContext: receiveContext, owner: d, id: id, supported: supported, sendGate: make(chan struct{}, 1)}
	r.abortContext, r.cancelAbort = context.WithCancelCause(context.Background())
	r.sendContext, r.cancelSend = context.WithCancelCause(context.Background())
	r.sendGate <- struct{}{}
	r.observeReceiveLocked()
	d.requests[id] = r
	return r, nil
}

// Deliver copies accepted payloads. Over-limit payloads and full queues drop the
// newest datagram. Malformed IDs return a connection-scoped h3.Error; unsupported
// registered requests return DatagramRequestError and discard their receive state.
func (d *DatagramDispatch) Deliver(data []byte) error {
	id, payload, err := h3.ParseDatagram(data)
	if err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	r := d.requests[id]
	if r == nil {
		return nil
	}
	r.observeReceiveLocked()
	if r.closed {
		return nil
	}
	if r.pending {
		r.pendingSeen = true
	}
	if !r.pending && !r.supported {
		r.closeLocked()
		return &DatagramRequestError{ID: id}
	}
	if len(payload) > d.maxPayload || len(r.queue) >= d.maxQueued {
		return nil
	}
	// make preserves an empty datagram as a queued item distinct from no item.
	owned := make([]byte, len(payload))
	copy(owned, payload)
	r.queue = append(r.queue, owned)
	r.notifyLocked()
	return nil
}
func (r *DatagramRequest) Receive() ([]byte, error) {
	d := r.owner
	d.mu.Lock()
	defer d.mu.Unlock()
	return r.receiveLocked()
}
func (r *DatagramRequest) receiveLocked() ([]byte, error) {
	r.observeReceiveLocked()
	if r.closed {
		return nil, ErrDatagramRequestClosed
	}
	if r.pending || len(r.queue) == 0 {
		return nil, ErrNoHTTPDatagram
	}
	data := r.queue[0]
	copy(r.queue, r.queue[1:])
	r.queue[len(r.queue)-1] = nil
	r.queue = r.queue[:len(r.queue)-1]
	return data, nil
}
func (r *DatagramRequest) CloseReceive() {
	d := r.owner
	d.mu.Lock()
	defer d.mu.Unlock()
	r.closeLocked()
}
func (r *DatagramRequest) Remove() {
	r.CloseSend()
	d := r.owner
	d.mu.Lock()
	defer d.mu.Unlock()
	r.closeLocked()
	if d.requests[r.id] == r {
		delete(d.requests, r.id)
	}
}

// DatagramSource supplies owned QUIC DATAGRAM payloads and must honor ctx.
type DatagramSource interface {
	ReceiveDatagram(context.Context) ([]byte, error)
}

// Run drains transport datagrams until cancellation or a connection-level error.
// The owner must stop/join this loop with the session and close the connection
// for a returned h3.Error. abortRequest must honor ctx and reset both directions
// of the indicated request. No callbacks execute while the queue mutex is held.
func (d *DatagramDispatch) Run(ctx context.Context, source DatagramSource, abortRequest func(context.Context, uint64, uint64) error) error {
	if source == nil || abortRequest == nil {
		return errors.New("http3: invalid datagram receiver")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		data, err := source.ReceiveDatagram(ctx)
		if err != nil {
			return err
		}
		err = d.Deliver(data)
		var request *DatagramRequestError
		if errors.As(err, &request) {
			if err := abortRequest(ctx, request.ID, request.Code()); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return err
		}
	}
}

// notifyLocked broadcasts a state change without retaining per-datagram signals.
// It allocates no channel until a receiver actually has to wait.
func (r *DatagramRequest) notifyLocked() {
	if r.changed != nil {
		close(r.changed)
		r.changed = nil
	}
}
func (r *DatagramRequest) closeLocked() {
	r.closed = true
	r.queue = nil
	r.notifyLocked()
}

// ReceiveContext blocks until a payload arrives or the request/context closes.
// Cancellation of one wait does not close the request or consume queued data.
func (r *DatagramRequest) ReceiveContext(ctx context.Context) ([]byte, error) {
	d := r.owner
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		d.mu.Lock()
		data, err := r.receiveLocked()
		if !errors.Is(err, ErrNoHTTPDatagram) {
			d.mu.Unlock()
			return data, err
		}
		if r.changed == nil {
			r.changed = make(chan struct{})
		}
		changed := r.changed
		d.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Close releases queued payloads and wakes all receivers. The session owner calls
// it during shutdown; subsequent registrations fail. It is idempotent.
func (d *DatagramDispatch) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	for _, r := range d.requests {
		r.cancelSend(ErrDatagramSendClosed)
		r.closeLocked()
	}
	clear(d.requests)
}

// CloseSend must precede transport FIN/RESET_STREAM. It joins an already admitted
// send after interrupting its queue-capacity wait; receive remains independent.
func (r *DatagramRequest) CloseSend() {
	r.cancelSend(ErrDatagramSendClosed)
	<-r.sendGate
	defer func() { r.sendGate <- struct{}{} }()
	r.sendClosed = true
}

// Check synchronously as well as observing cancellation asynchronously: a queued
// callback must not allow late delivery, or abort an already closed request.
func (r *DatagramRequest) observeReceiveLocked() {
	if !r.closed && r.receiveContext != nil && r.receiveContext.Err() != nil {
		r.closeLocked()
	}
}

// AbortRequest resets a bound stream after a request-scoped datagram error. A
// concurrently removed request needs no further action. No transport calls run
// under the dispatcher mutex.
func (d *DatagramDispatch) AbortRequest(ctx context.Context, id, code uint64) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	d.mu.Lock()
	r := d.requests[id]
	var abort func(uint64)
	if r != nil {
		abort = r.abort
	}
	d.mu.Unlock()
	if r != nil {
		r.cancelAbort(&DatagramRequestError{ID: id})
		r.CloseSend()
		r.CloseReceive()
		if abort != nil {
			abort(code)
		}
	}
	return nil
}

// resolve publishes request semantics after initial headers are validated. An
// unsupported request must fail even if its earlier payload was dropped for a
// size/queue limit. Closed receive sides discard their pending payloads.
func (r *DatagramRequest) resolve(supported bool) error {
	d := r.owner
	d.mu.Lock()
	defer d.mu.Unlock()
	if !r.pending {
		return ErrRequestID
	}
	r.observeReceiveLocked()
	r.pending, r.supported = false, supported
	if !r.closed && !supported && r.pendingSeen {
		r.closeLocked()
		return &DatagramRequestError{ID: r.id}
	}
	r.notifyLocked()
	return nil
}
