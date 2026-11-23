package transport

// forwardPublication is guarded by packetHandoff.mu. The connection owner polls
// acknowledgements; source owners only remove records when unsubscribing.
type forwardPublication struct {
	requested, installed uint64
	result               <-chan error
	err                  error
}

// publishForwardCIDs runs on the connection owner. It synchronizes the local
// snapshot and starts or polls foreign reservations without waiting for another
// owner. Readiness applies only to this snapshot and currently bound routes;
// it must be rechecked after CID-changing events and before packet construction.
// Once publication starts, it exclusively manages CID updates for bound routes;
// callers must not also submit manual updates to those registrations.
// The caller must arrange another poll on notify. The endpoint holds CID
// advertisements while pending; this is not a fatal Router.Sync error.
func (r *Router) publishForwardCIDs(c *Conn, notify chan<- struct{}) (bool, error) {
	if err := r.Sync(c); err != nil {
		return false, err
	}
	snapshot, ok := r.entries[c]
	if !ok {
		return false, ErrEndpointClosed
	}
	if r.handoff == nil {
		return true, nil
	}
	h := r.handoff
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false, ErrEndpointClosed
	}
	ready := true
	for route, publication := range h.connections[c] {
		route.publishResetHints(c)
		if route.revoked.Load() {
			return false, ErrEndpointClosed
		}
		if snapshot.zero {
			return false, ErrRouteCollision
		}
		if publication.result != nil {
			select {
			case err := <-publication.result:
				publication.result = nil
				publication.err = err
				if err == nil {
					publication.installed = publication.requested
				}
			default:
			}
		}
		if publication.err != nil {
			return false, publication.err
		}
		if publication.installed == snapshot.revision {
			continue
		}
		ready = false
		if publication.result != nil {
			continue
		}
		ids := make([][]byte, len(snapshot.ids))
		for i, id := range snapshot.ids {
			ids[i] = []byte(id)
		}
		result, err := route.queueCIDUpdate(ids, notify)
		if err != nil {
			return false, err
		}
		publication.requested = snapshot.revision
		publication.result = result
	}
	return ready, nil
}
