package transport

import (
	"errors"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func TestCrossPathReliableACK(t *testing.T) {
	for _, ackPath := range []uint64{0, 1} {
		p, a, b := pathRecoveryPair(t)
		now := time.Unix(100, 0)
		item := &sendItem{frame: wire.StreamFrame{StreamID: 0, Data: []byte("reliable data")}, bytes: 13}
		sendPathPacket(t, a, now)
		a.space.sent[0] = item
		// Loss sends the same logical data on another path; retain the old mapping.
		queueLostItems(a.space, b.space, []recovery.Packet{{Number: 0}})
		queueLostItems(a.space, b.space, []recovery.Packet{{Number: 0}})
		if len(b.space.queue) != 1 || b.space.queue[0] != item || !item.queued {
			t.Fatal("duplicate loss queue")
		}
		sendPathPacket(t, b, now.Add(time.Millisecond))
		b.space.sent[0] = item
		b.space.queue = nil
		item.queued = false
		completed, bytesReleased := 0, 0
		complete := func(got *sendItem) error { completed++; bytesReleased += got.bytes; return nil }
		f := wire.PathAckFrame{PathID: ackPath, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}}}
		if _, err := p.receiveReliableACK(f, now.Add(10*time.Millisecond), 3, 0, false, complete); err != nil {
			t.Fatal(err)
		}
		// The ACKed path's entry leaves its ledger; the other path's leaves
		// when its own ACK, loss or reclaim reaches it.
		acked, other := b, a
		if ackPath == 0 {
			acked, other = a, b
		}
		if completed != 1 || bytesReleased != 13 || !item.acked || len(acked.space.sent) != 0 {
			t.Fatal("completion not shared")
		}
		// ACK of reliable data does not remove another path's packet from flight.
		if other.sent.BytesInFlight != 1200 {
			t.Fatal("frame delivery changed other path flight")
		}
		queueLostItems(other.space, other.space, []recovery.Packet{{Number: 0}})
		if len(other.space.queue) != 0 {
			t.Fatal("late loss requeued delivered data")
		}
		f.PathID = 1 - ackPath
		if _, err := p.receiveReliableACK(f, now.Add(20*time.Millisecond), 3, 0, false, complete); err != nil {
			t.Fatal(err)
		}
		if completed != 1 || a.sent.BytesInFlight != 0 || b.sent.BytesInFlight != 0 {
			t.Fatal("duplicate completion/accounting")
		}
		if len(a.space.sent) != 0 || len(b.space.sent) != 0 || len(a.space.untracked) != 0 || len(b.space.untracked) != 0 {
			t.Fatal("ledgers not emptied once every path's packet was settled", len(a.space.sent), len(b.space.sent))
		}
	}
}

func TestLateReliableACKAfterRecoveryLoss(t *testing.T) {
	p, a, b := pathRecoveryPair(t)
	now := time.Unix(100, 0)
	item := &sendItem{frame: wire.PingFrame{}, bytes: 1}
	sendPathPacket(t, a, now)
	a.space.sent[0] = item
	for i := 0; i < 3; i++ {
		sendPathPacket(t, a, now)
	}
	f := wire.PathAckFrame{AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 3, Largest: 3}}}}
	completed := 0
	complete := func(*sendItem) error { completed++; return nil }
	events, err := p.receiveReliableACK(f, now.Add(time.Millisecond), 3, 0, false, complete)
	if err != nil || len(events.Lost) != 1 || completed != 0 {
		t.Fatal(events, err)
	}
	queueLostItems(a.space, b.space, events.Lost)
	f.Ranges = []wire.AckRange{{Smallest: 0, Largest: 0}}
	events, err = p.receiveReliableACK(f, now.Add(2*time.Millisecond), 3, 0, false, complete)
	if err != nil || len(events.Acked) != 0 || completed != 1 || !b.space.queue[0].acked {
		t.Fatal("late ACK not applied to queued copy", events, err)
	}
}

func TestReliableACKInvalidAndCompletionFailure(t *testing.T) {
	p, a, _ := pathRecoveryPair(t)
	now := time.Unix(100, 0)
	sendPathPacket(t, a, now)
	item := &sendItem{frame: wire.PingFrame{}}
	a.space.sent[0] = item
	called := 0
	failure := errors.New("stream completion failed")
	complete := func(*sendItem) error { called++; return failure }
	f := wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 1, Largest: 1}}}
	if _, err := p.receiveReliableACK(f, now, 3, 0, false, complete); err == nil || called != 0 || item.acked {
		t.Fatal("invalid ACK completed data")
	}
	f.Ranges[0] = wire.AckRange{Smallest: 0, Largest: 0}
	if _, err := p.receiveReliableACK(f, now, 3, 0, false, complete); !errors.Is(err, failure) || called != 1 || item.acked {
		t.Fatal("completion failure swallowed", err)
	}
}

func TestAbandonmentTransfersReliableData(t *testing.T) {
	p, _, a := pathRecoveryPair(t)
	now := time.Unix(100, 0)
	sent := &sendItem{frame: wire.PingFrame{}, bytes: 1}
	queued := &sendItem{frame: wire.PingFrame{}, bytes: 2, queued: true}
	delivered := &sendItem{frame: wire.PingFrame{}, acked: true}
	elsewhere := &sendItem{frame: wire.PingFrame{}, bytes: 3, queued: true}
	sendPathPacket(t, a, now)
	a.space.sent[0] = sent
	// Previously lost packets may no longer be in the recovery ledger.
	a.space.sent[12] = sent
	a.space.sent[13] = elsewhere
	a.space.sent[14] = delivered
	a.space.queue = []*sendItem{queued, delivered}
	p.beginAbandon(1)
	p.receiveAbandon(wire.PathAbandonFrame{PathID: 1}, now, time.Second)
	got := p.collectAbandoned(now.Add(3 * time.Second))
	if len(got) != 1 || len(got[0].Reliable) != 3 || len(got[0].Lost) != 1 {
		t.Fatal(got)
	}
	seen := map[*sendItem]bool{}
	for _, item := range got[0].Reliable {
		if seen[item] || item.acked {
			t.Fatal("duplicate/delivered transfer")
		}
		seen[item] = true
	}
	if !seen[sent] || !seen[queued] || !seen[elsewhere] || queued.queued || !elsewhere.queued {
		t.Fatal("wrong queue ownership on transfer")
	}
	if len(a.space.sent) != 0 || len(a.space.queue) != 0 {
		t.Fatal("disposed path retained payloads")
	}
}

// Acknowledging one packet costs the same whatever the ledger's size: ACK
// processing visits the newly acknowledged packets, not every entry.
func BenchmarkAcknowledgeOneOfMany(b *testing.B) {
	for _, n := range []int{1000, 30000} {
		b.Run(itoaBench(n), func(b *testing.B) {
			s := &numberSpace{sent: make(map[uint64]*sendItem, n)}
			for pn := uint64(0); pn < uint64(n); pn++ {
				s.sent[pn] = &sendItem{frame: wire.PingFrame{}}
			}
			complete := func(*sendItem) error { return nil }
			b.ResetTimer()
			for i := range b.N {
				pn := uint64(i % n)
				if _, ok := s.sent[pn]; !ok {
					s.sent[pn] = &sendItem{frame: wire.PingFrame{}}
				}
				ack := wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: uint64(n - 1)}}}
				if err := acknowledgeItems(s, ack, []recovery.Packet{{Number: pn}}, complete); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func itoaBench(n int) string {
	if n >= 1000 {
		return itoaBench(n/1000) + "k"
	}
	b := []byte{}
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
