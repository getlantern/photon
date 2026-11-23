package transport

import "errors"

// handoffCIDUpdate owns a copied CID set until the source owner applies it.
// Exactly one result is delivered; neither owner waits for the other owner.
type handoffCIDUpdate struct {
	ids    [][]byte
	result chan error
	notify chan<- struct{}
}

// queueCIDUpdate is safe from a connection-owner callback. The caller must wait
// for a successful result before advertising the submitted IDs, and must still
// check route revocation. A success acknowledges installation, not future life.
// One request per route and maxForwardCIDs pending IDs per source bound storage.
// Automatic CID publication and packet-send gating are separate responsibilities.
func (route *handoffRoute) queueCIDUpdate(ids [][]byte, notify chan<- struct{}) (<-chan error, error) {
	if len(ids) > maxForwardCIDs {
		return nil, ErrRouterCapacity
	}
	for _, id := range ids {
		if len(id) == 0 || len(id) > 20 {
			return nil, errors.New("quic: invalid forwarding CID")
		}
	}
	route.updateMu.Lock()
	defer route.updateMu.Unlock()
	if route.revoked.Load() {
		return nil, ErrEndpointClosed
	}
	if route.pending != nil {
		return nil, errOperationWouldBlock
	}
	count := int64(len(ids))
	if route.router.forwardPendingIDs.Add(count) > maxForwardCIDs {
		route.router.forwardPendingIDs.Add(-count)
		return nil, ErrRouterCapacity
	}
	request := &handoffCIDUpdate{ids: make([][]byte, len(ids)), result: make(chan error, 1), notify: notify}
	for i, id := range ids {
		request.ids[i] = append([]byte(nil), id...)
	}
	route.pending = request
	select {
	case route.wake <- struct{}{}:
	default:
	}
	return request.result, nil
}

// finishCIDUpdate may race revocation with source application. A nil request
// cancels whatever is pending; the pending pointer ensures only one completion.
func (route *handoffRoute) finishCIDUpdate(request *handoffCIDUpdate, err error) {
	route.updateMu.Lock()
	defer route.updateMu.Unlock()
	pending := route.pending
	if pending == nil || (request != nil && request != pending) {
		return
	}
	route.pending = nil
	route.router.forwardPendingIDs.Add(-int64(len(pending.ids)))
	pending.result <- err
	select {
	case pending.notify <- struct{}{}:
	default:
	}
}

// applyForwardUpdates runs only on the source owner, after the coalesced wakeup.
// Keep the pending pointer installed during application to exclude a second
// request until this one is completed or revoked. No request mutex is held while
// touching routing maps or subscribing/unsubscribing on a destination inbox.
func (r *Router) applyForwardUpdates() {
	for route := range r.forwardRegistrations {
		route.updateMu.Lock()
		request := route.pending
		route.updateMu.Unlock()
		if request == nil {
			continue
		}
		err := r.setForwardCIDs(route, request.ids)
		route.finishCIDUpdate(request, err)
	}
	// Capacity is shared across destinations. Completion or prior revocation
	// can unblock a publisher whose request was rejected before it could queue
	// its own notification. Wake every remaining destination to retry.
	for route := range r.forwardRegistrations {
		route.target.signalPublication()
	}
}
