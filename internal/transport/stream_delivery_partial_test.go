package transport

import (
	"errors"
	"reflect"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestStreamDeliveryPartialPrefix(t *testing.T) {
	for _, lateOriginal := range []bool{false, true} {
		d, _ := newStreamDelivery(wire.StreamFrame{Offset: 100, Data: make([]byte, 130), Fin: true})
		calls := 0
		complete := func() error { calls++; return nil }
		// ACK only suffix bytes first; this cannot satisfy the reliable prefix.
		if done, err := d.acknowledge(164, 66, true, complete); done || err != nil {
			t.Fatal(done, err)
		}
		if done, err := d.retainPrefix(165, complete); done || err != nil || d.remaining != 64 {
			t.Fatal(done, err, d.remaining)
		}
		// ACKing the abandoned suffix again must not change required coverage.
		if done, err := d.acknowledge(200, 30, true, complete); done || err != nil || d.remaining != 64 {
			t.Fatal(done, err, d.remaining)
		}
		if lateOriginal {
			if done, err := d.acknowledge(100, 130, true, complete); !done || err != nil {
				t.Fatal(done, err)
			}
		} else {
			// The trimmed retransmission has no FIN; overlap at the boundary is safe.
			if done, err := d.acknowledge(100, 65, false, complete); !done || err != nil {
				t.Fatal(done, err)
			}
		}
		if calls != 1 || d.remaining != 0 {
			t.Fatal(calls, d.remaining)
		}
		if done, err := d.acknowledge(100, 130, true, complete); done || err != nil || calls != 1 {
			t.Fatal("late duplicate", done, err, calls)
		}
	}
}

func TestStreamDeliveryPartialReductionAtomic(t *testing.T) {
	d, _ := newStreamDelivery(wire.StreamFrame{Offset: 100, Data: make([]byte, 130), Fin: true})
	calls := 0
	complete := func() error { calls++; return nil }
	if _, err := d.acknowledge(100, 64, false, complete); err != nil {
		t.Fatal(err)
	}
	if done, err := d.retainPrefix(170, complete); done || err != nil || d.remaining != 6 {
		t.Fatal(done, err, d.remaining)
	}
	before := *d
	before.covered = append([]uint64(nil), d.covered...)
	fail := errors.New("completion failed")
	if _, err := d.retainPrefix(164, func() error { return fail }); !errors.Is(err, fail) || !reflect.DeepEqual(before, *d) {
		t.Fatal("failed reduction mutated ledger", err)
	}
	if _, err := d.retainPrefix(171, complete); err == nil || !reflect.DeepEqual(before, *d) {
		t.Fatal("increased reliable prefix")
	}
	if done, err := d.retainPrefix(164, complete); !done || err != nil || calls != 1 || d.finACKed {
		t.Fatal(done, err, calls)
	}
	// Full original FIN remains a valid late ACK despite removed obligations.
	if _, err := d.acknowledge(100, 130, true, complete); err != nil || calls != 1 {
		t.Fatal(err, calls)
	}
}

func TestStreamDeliveryPartialZeroAndFINOnly(t *testing.T) {
	for _, length := range []int{0, 65} {
		d, _ := newStreamDelivery(wire.StreamFrame{Offset: 100, Data: make([]byte, length), Fin: true})
		calls := 0
		if done, err := d.retainPrefix(99, func() error { calls++; return nil }); !done || err != nil || calls != 1 || d.finACKed {
			t.Fatal(done, err, calls)
		}
		if _, err := d.acknowledge(100, length, true, func() error { calls++; return nil }); err != nil || calls != 1 {
			t.Fatal(err, calls)
		}
	}
}

func FuzzStreamDeliveryPartial(f *testing.F) {
	f.Add([]byte{130, 1, 0, 64, 0, 100, 0, 0, 1, 64, 66, 1})
	f.Add([]byte{65, 0, 64, 0, 0, 1, 0, 65, 0})
	f.Add([]byte{0, 0, 0, 0, 128, 0, 0, 0, 0})
	f.Add([]byte{65, 1, 0, 65, 129, 1, 0, 65, 1})
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) == 0 {
			return
		}
		size := int(input[0])
		d, _ := newStreamDelivery(wire.StreamFrame{Offset: 12, Data: make([]byte, size), Fin: true})
		covered := make([]bool, size)
		required, partial, finSeen, calls := size, false, false, 0
		failure := errors.New("completion rejected")
		for i := 1; i+3 < len(input) && i < 401; i += 4 {
			before := *d
			before.covered = append([]uint64{}, d.covered...)
			complete := func() error {
				if input[i+3]&128 != 0 {
					return failure
				}
				calls++
				return nil
			}
			unchangedOnFailure := func(err error) bool {
				if !errors.Is(err, failure) {
					return false
				}
				if !reflect.DeepEqual(before, *d) {
					t.Fatal("failed callback changed delivery state")
				}
				return true
			}
			if input[i]&1 == 0 {
				next := int(input[i+1]) % (required + 1)
				if _, err := d.retainPrefix(uint64(12+next), complete); err != nil {
					if unchangedOnFailure(err) {
						continue
					}
					t.Fatal(err)
				}
				required, partial = next, true
			} else {
				start := int(input[i+1]) % (size + 1)
				length := int(input[i+2]) % (size - start + 1)
				fin := input[i+3]&1 != 0 && start+length == size
				if _, err := d.acknowledge(uint64(12+start), length, fin, complete); err != nil {
					if unchangedOnFailure(err) {
						continue
					}
					t.Fatal(err)
				}
				for j := start; j < start+length; j++ {
					covered[j] = true
				}
				finSeen = finSeen || fin
			}
			want := partial || finSeen
			for _, v := range covered[:required] {
				want = want && v
			}
			if d.completed != want || calls > 1 || want && calls != 1 {
				t.Fatal("prefix/FIN completion mismatch", required, calls)
			}
		}
	})
}

// Packet-number mappings for old and trimmed transmissions share one logical
// item. This exercises only the ACK/ownership machinery; queue rewriting and
// ResetAt wiring live in stream_item_partial.go and partial_reset_send.go.
func TestStreamDeliveryPartialPacketOwnership(t *testing.T) {
	for _, originalACK := range []bool{false, true} {
		c, s, root := splitItemFixture(t)
		calls := 0
		complete := func(item *sendItem) error {
			if item != root {
				t.Fatal("fragment completed independently")
			}
			calls++
			c.queuedBytes -= item.bytes
			return nil
		}
		ack := func(pn uint64) {
			t.Helper()
			if err := acknowledgeItems(s, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: pn, Largest: pn}}}, complete); err != nil {
				t.Fatal(err)
			}
		}
		ack(3) // Abandoned suffix may already have reached the peer.
		d := root.delivery
		if done, err := d.ledger.retainPrefix(1200, func() error { return complete(root) }); done || err != nil {
			t.Fatal(done, err)
		}
		ack(1) // Prefix through 1140; the last 60 required bytes are still missing.
		if calls != 0 || c.queuedBytes != root.bytes {
			t.Fatal("prefix gap released ownership")
		}
		middle := s.sent[2].frame.(wire.StreamFrame)
		trimmed := middle
		trimmed.Data = middle.Data[:60]
		item := &sendItem{frame: trimmed, delivery: d}
		d.items = append(d.items, item)
		s.sent[4] = item
		if originalACK {
			ack(0)
		} else {
			ack(4)
		}
		ack(0)
		ack(2)
		ack(4)
		pruneAcknowledged(s)
		if calls != 1 || c.queuedBytes != 0 || len(s.sent) != 0 {
			t.Fatal("partial completion ownership", calls, c.queuedBytes, len(s.sent))
		}
		for _, part := range d.items {
			if !part.acked {
				t.Fatal("shared fragment retained")
			}
		}
	}
}
