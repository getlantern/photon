package transport

import (
	"errors"
	"net/netip"

	"github.com/getlantern/photon/internal/recovery"
)

var errWriteReservationUsed = errors.New("quic: write reservation already consumed")

// foreignWriteReservation guarantees one queue slot against other producers.
// It must be submitted or canceled. Closure/revocation can invalidate it; those
// are lifetime failures, not queue pressure after packet construction.
// Identity fields are immutable; all lifecycle state lives under writer.mu.
type foreignWriteReservation struct {
	writer *packetWriter
	route  *handoffRoute
}

func (w *packetWriter) reserve(route *handoffRoute) (*foreignWriteReservation, error) {
	if route == nil || route.router != w.router {
		return nil, errPathBinding
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed || route.revoked.Load() {
		return nil, ErrEndpointClosed
	}
	w.pruneReservationsLocked()
	if len(w.writes)+len(w.reserved) >= cap(w.writes) {
		if route.target != nil {
			if _, waiting := w.waiters[route]; !waiting && len(w.waiters) >= maxForwardCIDs {
				return nil, ErrRouterCapacity
			}
			if w.waiters == nil {
				w.waiters = make(map[*handoffRoute]struct{})
			}
			w.waiters[route] = struct{}{}
		}
		return nil, errOperationWouldBlock
	}
	if w.reserved == nil {
		w.reserved = make(map[*foreignWriteReservation]struct{})
	}
	reservation := &foreignWriteReservation{writer: w, route: route}
	delete(w.waiters, route)
	w.reserved[reservation] = struct{}{}
	return reservation, nil
}
func (reservation *foreignWriteReservation) cancel() {
	w := reservation.writer
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.reserved[reservation]; ok {
		delete(w.reserved, reservation)
		w.signalCapacityLocked()
	}
}

// submit consumes the ticket even on invalid input. A successful submission
// copies the packet before returning and cannot fail because another producer
// filled the queue. Cancel after submit does not cancel an accepted write.
func (reservation *foreignWriteReservation) submit(data []byte, peer netip.AddrPort, marking recovery.ECNMark, notify chan<- struct{}) (<-chan foreignWriteResult, error) {
	w := reservation.writer
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil, ErrEndpointClosed
	}
	if _, ok := w.reserved[reservation]; !ok {
		if reservation.route.revoked.Load() {
			return nil, ErrEndpointClosed
		}
		return nil, errWriteReservationUsed
	}
	delete(w.reserved, reservation)
	if reservation.route.revoked.Load() {
		w.signalCapacityLocked()
		return nil, ErrEndpointClosed
	}
	if !peer.IsValid() || peer.Port() == 0 || len(data) == 0 || len(data) > 65535 || marking > recovery.ECT0 {
		w.signalCapacityLocked()
		return nil, errPathBinding
	}
	request := &foreignWrite{route: reservation.route, data: append([]byte(nil), data...), peer: canonicalPeer(peer), marking: marking, result: make(chan foreignWriteResult, 1), notify: notify}
	w.writes <- request // queue + reservations never exceeds channel capacity
	return request.result, nil
}
func (w *packetWriter) signalCapacityLocked() {
	for route := range w.waiters {
		route.target.signalPublication()
	}
	w.waiters = nil
}
func (w *packetWriter) capacityAvailable() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.signalCapacityLocked()
}
func (w *packetWriter) pruneReservationsLocked() {
	for route := range w.waiters {
		if route.revoked.Load() {
			delete(w.waiters, route)
		}
	}
	changed := false
	for reservation := range w.reserved {
		if reservation.route.revoked.Load() {
			delete(w.reserved, reservation)
			changed = true
		}
	}
	if changed {
		w.signalCapacityLocked()
	}
}
func (w *packetWriter) pruneReservations() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pruneReservationsLocked()
}
