package transport

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"syscall"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func pendingWriteFixture(t *testing.T) (*pendingForeignWrite, *packetWriter, *recovery.Sent) {
	t.Helper()
	r, _ := NewRouter(8, 4)
	route := &handoffRoute{router: r, target: newPacketHandoff()}
	w := newPacketWriter(r)
	t.Cleanup(w.close)
	sent, _ := recovery.NewSent(4)
	sent.EnableECN()
	if err := sent.OnSent(recovery.Application, recovery.Packet{ECN: recovery.ECT0, Bytes: 1200, InFlight: true, AckEliciting: true}); err != nil {
		t.Fatal(err)
	}
	ticket, err := w.reserve(route)
	if err != nil {
		t.Fatal(err)
	}
	out := &Outbound{Conn: &Conn{}, Peer: netip.MustParseAddrPort("127.0.0.1:443"), Data: []byte("ciphertext"), ECN: recovery.ECT0, ecn: ecnCommit{sent, recovery.Application, 0}}
	p, err := beginForeignWrite(ticket, out, route.target.changed)
	if err != nil {
		t.Fatal(err)
	}
	clear(out.Data)
	*out = Outbound{}
	return p, w, sent
}

func TestForeignCompletionECNRetryUnderPressure(t *testing.T) {
	p, w, sent := pendingWriteFixture(t)
	if _, _, ok := p.poll(); ok {
		t.Fatal("completed before write")
	}
	request := <-w.writes
	original := append([]byte(nil), request.data...)
	request.complete(0, errForeignECNUnsupported)
	for range cap(w.writes) {
		if _, err := w.offer(p.route, []byte{1}, p.out.Peer, recovery.NotECT, nil); err != nil {
			t.Fatal(err)
		}
	}
	for range 3 {
		if _, _, ok := p.poll(); ok {
			t.Fatal("completed while retry blocked")
		}
		if !p.retry || sent.ECNSent(recovery.Application) != [2]uint64{} || sent.BytesInFlight != 1200 {
			t.Fatal("incorrect ECN rollback")
		}
	}
	// Remove the original completion notification, then require a capacity wake.
	select {
	case <-p.route.target.changed:
	default:
	}
	queued := <-w.writes
	queued.complete(1, nil)
	w.capacityAvailable()
	select {
	case <-p.route.target.changed:
	default:
		t.Fatal("no capacity wake")
	}
	if _, _, ok := p.poll(); ok || p.retry {
		t.Fatal("retry not queued")
	}
	for range cap(w.writes) - 1 {
		q := <-w.writes
		q.complete(1, nil)
	}
	retry := <-w.writes
	if retry.marking != recovery.NotECT || !bytes.Equal(retry.data, original) {
		t.Fatal("retry changed packet")
	}
	retry.complete(len(retry.data), nil)
	out, result, ok := p.poll()
	if !ok || result.err != nil || result.n != len(original) || !bytes.Equal(out.Data, original) {
		t.Fatal("missing completion", result)
	}
	if _, _, ok := p.poll(); ok || p.out != nil || p.result != nil {
		t.Fatal("completion retained or repeated")
	}
	if sent.BytesInFlight != 1200 {
		t.Fatal("retry recommitted recovery state")
	}
}

func TestForeignCompletionOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name string
		n    int
		err  error
		want error
	}{
		{"success", 10, nil, nil}, {"short", 1, nil, io.ErrShortWrite}, {"empty", 0, nil, io.ErrShortWrite},
		{"partial-ecn", 1, errForeignECNUnsupported, errForeignECNUnsupported},
		{"partial-mtu", 1, syscall.EMSGSIZE, syscall.EMSGSIZE}, {"mtu", 0, syscall.EMSGSIZE, syscall.EMSGSIZE},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, w, sent := pendingWriteFixture(t)
			request := <-w.writes
			request.complete(tc.n, tc.err)
			_, result, ok := p.poll()
			if !ok || result.n != tc.n || !errors.Is(result.err, tc.want) {
				t.Fatal(result, ok)
			}
			if len(w.writes) != 0 || sent.ECNSent(recovery.Application) != [2]uint64{1, 0} {
				t.Fatal("unexpected retry or rollback")
			}
			if _, _, ok := p.poll(); ok {
				t.Fatal("duplicate completion")
			}
		})
	}
}

func TestForeignCompletionRetryIsSingleUse(t *testing.T) {
	p, w, _ := pendingWriteFixture(t)
	request := <-w.writes
	request.complete(0, errForeignECNUnsupported)
	p.poll()
	retry := <-w.writes
	retry.complete(0, errForeignECNUnsupported)
	_, result, ok := p.poll()
	if !ok || !errors.Is(result.err, errForeignECNUnsupported) || len(w.writes) != 0 {
		t.Fatal("second retry", result)
	}
}

func TestForeignCompletionClosure(t *testing.T) {
	for _, mode := range []string{"connection", "writer", "route", "retry-writer", "retry-route"} {
		t.Run(mode, func(t *testing.T) {
			p, w, sent := pendingWriteFixture(t)
			if mode == "connection" {
				p.out.Conn.closed = true
				request := <-w.writes
				request.complete(0, errForeignECNUnsupported)
				if _, _, ok := p.poll(); ok || !p.done || p.out != nil || sent.ECNSent(recovery.Application) != [2]uint64{1, 0} {
					t.Fatal("closed engine mutated")
				}
				return
			}
			if mode == "writer" {
				w.close()
			} else if mode == "route" {
				p.route.revoke()
				request := <-w.writes
				request.complete(0, ErrEndpointClosed)
			} else {
				request := <-w.writes
				request.complete(0, errForeignECNUnsupported)
				if mode == "retry-writer" {
					w.close()
				} else {
					p.route.revoke()
				}
				p.poll()
			}
			_, result, ok := p.poll()
			if !ok || !errors.Is(result.err, ErrEndpointClosed) {
				t.Fatal(result, ok)
			}
		})
	}
}

func TestForeignCompletionSubmissionFailure(t *testing.T) {
	p, w, _ := pendingWriteFixture(t)
	// Drain this fixture's first send; the second ticket is revoked before submit.
	request := <-w.writes
	request.complete(len(request.data), nil)
	out, _, ok := p.poll()
	if !ok {
		t.Fatal("missing first completion")
	}
	ticket, err := w.reserve(p.route)
	if err != nil {
		t.Fatal(err)
	}
	p.route.revoke()
	notify := make(chan struct{}, 1)
	failed, err := beginForeignWrite(ticket, out, notify)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-notify:
	default:
		t.Fatal("submission failure did not wake owner")
	}
	_, result, ok := failed.poll()
	if !ok || !errors.Is(result.err, ErrEndpointClosed) || len(w.writes) != 0 {
		t.Fatal(result, ok)
	}
}

func TestForeignCompletionMissingECNCommit(t *testing.T) {
	p, w, sent := pendingWriteFixture(t)
	p.out.ecn = ecnCommit{}
	request := <-w.writes
	request.complete(0, errForeignECNUnsupported)
	_, result, ok := p.poll()
	if !ok || !errors.Is(result.err, errForeignECNUnsupported) || len(w.writes) != 0 || sent.ECNSent(recovery.Application) != [2]uint64{1, 0} {
		t.Fatal("retried without accounting", result)
	}
}

func TestForeignCompletionRetainsPacketMetadata(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, kind := range []string{"probe", "stream", "datagram", "base-stream", "base-datagram"} {
			t.Run(fmt.Sprintf("%x/%s", uint32(v), kind), func(t *testing.T) {
				e, c, _, out := ownedOutboundFixture(t, v, kind)
				r, _ := NewRouter(8, 4)
				w := newPacketWriter(r)
				defer w.close()
				ticket, err := w.reserve(&handoffRoute{router: r})
				if err != nil {
					t.Fatal(err)
				}
				p, err := beginForeignWrite(ticket, out, nil)
				if err != nil {
					t.Fatal(err)
				}
				original := append([]byte(nil), out.Data...)
				clear(out.Data)
				*out = Outbound{}
				request := <-w.writes
				request.complete(0, syscall.EMSGSIZE)
				retained, result, ok := p.poll()
				if !ok || result.n != 0 || !errors.Is(result.err, syscall.EMSGSIZE) || !bytes.Equal(retained.Data, original) {
					t.Fatal(result, ok)
				}
				e.HandleWriteError(retained, result.err, time.Unix(102, 0))
				if c.closed {
					t.Fatal("lost write metadata")
				}
				if c.multipath == nil {
					if c.recovery.BytesInFlight != 0 || !c.baseMTUNeedsValidation {
						t.Fatal("missing base fallback")
					}
				} else if c.multipath.paths[1].application.sent.BytesInFlight != 0 {
					t.Fatal("missing path fallback")
				}
			})
		}
	}
}
