package transport

import (
	"context"
	"errors"
	"testing"
	"time"
)

func updateTestRoute(t *testing.T, r *Router, h *packetHandoff, ids [][]byte) *handoffRoute {
	t.Helper()
	route := &handoffRoute{router: r, target: h, wake: make(chan struct{}, 1)}
	if !h.subscribe(route) {
		t.Fatal("subscribe")
	}
	if err := r.setForwardCIDs(route, ids); err != nil {
		t.Fatal(err)
	}
	if r.forwardRegistrations == nil {
		r.forwardRegistrations = make(map[*handoffRoute]struct{})
	}
	r.forwardRegistrations[route] = struct{}{}
	return route
}
func updateResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("missing update acknowledgement")
		return nil
	}
}
func TestForwardUpdateAtomicity(t *testing.T) {
	r, _ := NewRouter(8, 8)
	h := newPacketHandoff()
	defer h.close()
	a := updateTestRoute(t, r, h, [][]byte{{1}})
	updateTestRoute(t, r, h, [][]byte{{9}})
	notify := make(chan struct{}, 1)
	ids := [][]byte{{2, 3}}
	result, err := a.queueCIDUpdate(ids, notify)
	if err != nil {
		t.Fatal(err)
	}
	ids[0][0] = 8
	if r.forwardTarget([]byte{0, 1, 0}) != h || r.forwardTarget([]byte{0, 2, 3, 0}) != nil {
		t.Fatal("unacknowledged update changed dispatch")
	}
	if _, err := a.queueCIDUpdate(nil, nil); !errors.Is(err, errOperationWouldBlock) {
		t.Fatal("unbounded pending requests", err)
	}
	r.applyForwardUpdates()
	if err := updateResult(t, result); err != nil {
		t.Fatal(err)
	}
	select {
	case <-notify:
	default:
		t.Fatal("completion did not wake requester")
	}
	if r.forwardTarget([]byte{0, 1, 0}) != nil || r.forwardTarget([]byte{0, 2, 3, 0}) != h {
		t.Fatal("CID copy/replacement")
	}
	result, err = a.queueCIDUpdate([][]byte{{9, 1}}, notify)
	if err != nil {
		t.Fatal(err)
	}
	r.applyForwardUpdates()
	if err := updateResult(t, result); !errors.Is(err, ErrRouteCollision) {
		t.Fatal("collision acknowledged as installed", err)
	}
	if r.forwardTarget([]byte{0, 2, 3, 0}) != h {
		t.Fatal("rejected update lost prior route")
	}
	// Full notifications cannot block completion.
	result, err = a.queueCIDUpdate(nil, notify)
	if err != nil {
		t.Fatal(err)
	}
	r.applyForwardUpdates()
	if err := updateResult(t, result); err != nil {
		t.Fatal(err)
	}
	if len(a.ids) != 0 || r.forwardPendingIDs.Load() != 0 {
		t.Fatal("empty update or accounting")
	}
}
func TestForwardUpdateBoundsAndRevocation(t *testing.T) {
	for _, mode := range []string{"source", "destination", "connection"} {
		t.Run(mode, func(t *testing.T) {
			r, _ := NewRouter(8, 8)
			h := newPacketHandoff()
			defer h.close()
			a := updateTestRoute(t, r, h, nil)
			b := updateTestRoute(t, r, h, nil)
			c := &Conn{}
			if err := h.bindConnection(a, c); err != nil {
				t.Fatal(err)
			}
			for _, invalid := range [][][]byte{{{}}, {make([]byte, 21)}, make([][]byte, maxForwardCIDs+1)} {
				if _, err := a.queueCIDUpdate(invalid, nil); err == nil {
					t.Fatal("invalid update queued")
				}
			}
			ids := make([][]byte, maxForwardCIDs)
			for i := range ids {
				ids[i] = []byte{1}
			}
			result, err := a.queueCIDUpdate(ids, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := b.queueCIDUpdate([][]byte{{2}}, nil); !errors.Is(err, ErrRouterCapacity) {
				t.Fatal("aggregate pending cap", err)
			}
			switch mode {
			case "source":
				r.removeForwardRoute(a)
			case "destination":
				h.close()
			case "connection":
				h.revokeConnection(c)
			}
			if err := updateResult(t, result); !errors.Is(err, ErrEndpointClosed) {
				t.Fatal("revoked request completed successfully", err)
			}
			r.pruneForwardRoutes()
			r.applyForwardUpdates()
			select {
			case <-result:
				t.Fatal("duplicate completion")
			default:
			}
			if r.forwardPendingIDs.Load() != 0 {
				t.Fatal("pending capacity leaked")
			}
			if _, err := a.queueCIDUpdate(nil, nil); !errors.Is(err, ErrEndpointClosed) {
				t.Fatal("revoked handle queued", err)
			}
			if mode != "destination" {
				result, err = b.queueCIDUpdate([][]byte{{2}}, nil)
				if err != nil {
					t.Fatal("released capacity not reusable", err)
				}
				r.applyForwardUpdates()
				if err := updateResult(t, result); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
func TestForwardUpdateReciprocalOwners(t *testing.T) {
	a, b := forwardingRuntime(t), forwardingRuntime(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ah, err := a.handoffTarget(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bh, err := b.handoffTarget(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ar, err := a.registerHandoffRoute(ctx, bh, nil)
	if err != nil {
		t.Fatal(err)
	}
	br, err := b.registerHandoffRoute(ctx, ah, nil)
	if err != nil {
		t.Fatal(err)
	}
	for n := range 32 {
		type submitted struct {
			result <-chan error
			err    error
		}
		submissions := make(chan submitted, 2)
		for _, item := range []struct {
			owner  *UDPServer
			route  *handoffRoute
			prefix byte
		}{{a, br, 1}, {b, ar, 2}} {
			go func() {
				var result <-chan error
				err := item.owner.Do(ctx, func(*ServerEndpoint) error {
					var err error
					result, err = item.route.queueCIDUpdate([][]byte{{item.prefix, byte(n)}}, nil)
					return err
				})
				submissions <- submitted{result, err}
			}()
		}
		for range 2 {
			select {
			case submission := <-submissions:
				if submission.err != nil {
					t.Fatal(submission.err)
				}
				if err := updateResult(t, submission.result); err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("reciprocal owner update stalled")
			}
		}
	}
}

func TestForwardUpdateConcurrentDestinationClose(t *testing.T) {
	for range 32 {
		source, destination := forwardingRuntime(t), forwardingRuntime(t)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		h, err := destination.handoffTarget(ctx)
		if err != nil {
			t.Fatal(err)
		}
		route, err := source.registerHandoffRoute(ctx, h, [][]byte{{1}})
		if err != nil {
			t.Fatal(err)
		}
		result, err := route.queueCIDUpdate([][]byte{{2}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() { destination.Close(); close(done) }()
		if err := updateResult(t, result); err != nil && !errors.Is(err, ErrEndpointClosed) {
			t.Fatal(err)
		}
		select {
		case <-done:
		case <-ctx.Done():
			t.Fatal("destination close stalled")
		}
		if err := source.Wait(ctx, func(e *ServerEndpoint) (bool, error) {
			return len(e.router.foreignIDs) == 0 && len(e.router.forwardRegistrations) == 0 && e.router.forwardPendingIDs.Load() == 0, nil
		}); err != nil {
			t.Fatal("update/shutdown retained reservations", err)
		}
		select {
		case <-result:
			t.Fatal("duplicate update acknowledgement")
		default:
		}
		cancel()
	}
}
