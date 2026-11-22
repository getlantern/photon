package transport

import (
	"context"
	"errors"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
)

func TestForwardWriteReservedCapacity(t *testing.T) {
	r, _ := NewRouter(8, 4)
	route := &handoffRoute{router: r}
	w := newPacketWriter(r)
	defer w.close()
	peer := netip.MustParseAddrPort("127.0.0.1:443")
	reserved, err := w.reserve(route)
	if err != nil {
		t.Fatal(err)
	}
	for range 63 {
		if _, err := w.offer(route, []byte{1}, peer, recovery.NotECT, nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.offer(route, []byte{1}, peer, recovery.NotECT, nil); !errors.Is(err, errOperationWouldBlock) {
		t.Fatal("stole reserved capacity", err)
	}
	if _, err := w.reserve(route); !errors.Is(err, errOperationWouldBlock) {
		t.Fatal("over-reserved", err)
	}
	payload := []byte("reserved")
	result, err := reserved.submit(payload, peer, recovery.NotECT, nil)
	if err != nil {
		t.Fatal("reservation lost to competitors", err)
	}
	payload[0] = 'X'
	reserved.cancel() // Already-submitted work remains owned by the writer.
	if _, err := reserved.submit([]byte{1}, peer, recovery.NotECT, nil); !errors.Is(err, errWriteReservationUsed) {
		t.Fatal("ticket submitted twice", err)
	}
	for range 63 {
		request := <-w.writes
		request.complete(len(request.data), nil)
	}
	request := <-w.writes
	if string(request.data) != "reserved" {
		t.Fatal("reserved bytes not copied")
	}
	request.complete(len(request.data), nil)
	if got := foreignResult(t, result); got.n != 8 || got.err != nil {
		t.Fatal(got)
	}
	if len(w.reserved) != 0 {
		t.Fatal("submitted ticket retained")
	}
}

func TestForwardWriteReservationRelease(t *testing.T) {
	for _, mode := range []string{"cancel", "invalid", "revoked", "closed", "dequeue"} {
		t.Run(mode, func(t *testing.T) {
			r, _ := NewRouter(8, 4)
			h := newPacketHandoff()
			defer h.close()
			route := &handoffRoute{router: r}
			waiter := &handoffRoute{router: r, target: h}
			w := newPacketWriter(r)
			defer w.close()
			peer := netip.MustParseAddrPort("127.0.0.1:443")
			var tickets []*foreignWriteReservation
			for range 64 {
				ticket, err := w.reserve(route)
				if err != nil {
					t.Fatal(err)
				}
				tickets = append(tickets, ticket)
			}
			if _, err := w.reserve(waiter); !errors.Is(err, errOperationWouldBlock) {
				t.Fatal(err)
			}
			switch mode {
			case "cancel":
				tickets[0].cancel()
				tickets[0].cancel()
			case "invalid":
				if _, err := tickets[0].submit(nil, peer, recovery.NotECT, nil); !errors.Is(err, errPathBinding) {
					t.Fatal(err)
				}
			case "revoked":
				route.revoke()
				w.pruneReservations()
			case "closed":
				w.close()
			case "dequeue":
				result, err := tickets[0].submit([]byte{1}, peer, recovery.NotECT, nil)
				if err != nil {
					t.Fatal(err)
				}
				request := <-w.writes
				w.capacityAvailable()
				request.complete(1, nil)
				if got := foreignResult(t, result); got.n != 1 || got.err != nil {
					t.Fatal(got)
				}
			}
			select {
			case <-h.changed:
			default:
				t.Fatal("released capacity did not wake blocked publisher")
			}
			if mode == "closed" {
				if _, err := tickets[1].submit([]byte{1}, peer, recovery.NotECT, nil); !errors.Is(err, ErrEndpointClosed) {
					t.Fatal(err)
				}
				if len(w.reserved) != 0 {
					t.Fatal("closed writer retained tickets")
				}
			} else {
				ticket, err := w.reserve(waiter)
				if err != nil {
					t.Fatal("released capacity unavailable", err)
				}
				ticket.cancel()
			}
			if mode == "revoked" {
				if _, err := tickets[0].submit([]byte{1}, peer, recovery.NotECT, nil); !errors.Is(err, ErrEndpointClosed) {
					t.Fatal("revoked ticket submitted", err)
				}
			}
		})
	}
}

func TestForwardWriteReservationConcurrentSubmitCancel(t *testing.T) {
	r, _ := NewRouter(8, 4)
	route := &handoffRoute{router: r}
	w := newPacketWriter(r)
	defer w.close()
	peer := netip.MustParseAddrPort("127.0.0.1:443")
	for range 100 {
		ticket, err := w.reserve(route)
		if err != nil {
			t.Fatal(err)
		}
		var result <-chan foreignWriteResult
		var submitError error
		var wg sync.WaitGroup
		wg.Go(func() { result, submitError = ticket.submit([]byte{1}, peer, recovery.NotECT, nil) })
		wg.Go(ticket.cancel)
		wg.Wait()
		if submitError == nil {
			request := <-w.writes
			request.complete(1, nil)
			if got := foreignResult(t, result); got.err != nil || got.n != 1 {
				t.Fatal(got)
			}
		} else if !errors.Is(submitError, errWriteReservationUsed) {
			t.Fatal(submitError)
		}
		if len(w.reserved) != 0 || len(w.writes) != 0 {
			t.Fatal("submit/cancel retained capacity")
		}
	}
}

func TestForwardWriteRuntimeRevocationReleasesReservations(t *testing.T) {
	source, destination := forwardingRuntime(t), forwardingRuntime(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	h, err := destination.handoffTarget(ctx)
	if err != nil {
		t.Fatal(err)
	}
	route, err := source.registerHandoffRoute(ctx, h, nil)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := source.foreignWriteTarget(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var tickets []*foreignWriteReservation
	for range 64 {
		ticket, err := writer.reserve(route)
		if err != nil {
			t.Fatal(err)
		}
		tickets = append(tickets, ticket)
	}
	other := newPacketHandoff()
	defer other.close()
	waiting, err := source.registerHandoffRoute(ctx, other, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.reserve(waiting); !errors.Is(err, errOperationWouldBlock) {
		t.Fatal(err)
	}
	if err := destination.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-other.changed:
	case <-ctx.Done():
		t.Fatal("runtime revocation did not wake capacity waiter")
	}
	ticket, err := writer.reserve(waiting)
	if err != nil {
		t.Fatal("revoked reservations retained capacity", err)
	}
	ticket.cancel()
	for _, old := range tickets {
		if _, err := old.submit([]byte{1}, netip.MustParseAddrPort("127.0.0.1:443"), recovery.NotECT, nil); !errors.Is(err, ErrEndpointClosed) {
			t.Fatal("revoked reservation submitted", err)
		}
	}
}
