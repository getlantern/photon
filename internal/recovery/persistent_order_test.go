package recovery

import (
	"math/rand/v2"
	"testing"
	"time"
)

// Mix capacity eviction, prefix ACK retirement, space discard and local
// rejection against a slice ledger. The same test runs on the list control.
func TestCongestionHistoryOrderModel(t *testing.T) {
	for _, capacity := range []int{1, 2, 17} {
		h := congestionHistory{capacity: capacity}
		type entry struct {
			id   transmissionID
			done bool
		}
		var ledger []entry
		rng := rand.New(rand.NewPCG(12, uint64(capacity)))
		trim := func() {
			for len(ledger) > 0 && ledger[0].done {
				ledger = ledger[1:]
			}
		}
		for step := 0; step < 10000; step++ {
			space := Space(rng.IntN(3))
			switch rng.IntN(4) {
			case 0:
				id := transmissionID{space, uint64(step)}
				h.sent(space, Packet{Number: id.number, SentAt: time.Unix(int64(step), 0)}, true)
				if len(ledger) == capacity {
					ledger = ledger[1:]
				}
				ledger = append(ledger, entry{id: id})
			case 1:
				if len(ledger) > 0 {
					id := ledger[rng.IntN(len(ledger))].id
					h.ack(id.space, ack(id.number, id.number))
					for i := range ledger {
						if ledger[i].id == id {
							ledger[i].done = true
						}
					}
					trim()
				}
			case 2:
				h.discard(space)
				for i := range ledger {
					if ledger[i].id.space == space {
						ledger[i].done = true
					}
				}
				trim()
			case 3:
				if len(ledger) > 0 {
					i := rng.IntN(len(ledger))
					id := ledger[i].id
					h.order.Remove(h.index[id])
					delete(h.index, id)
					ledger = append(ledger[:i], ledger[i+1:]...)
				}
			}
			if h.order.Len() != len(ledger) || len(h.index) != len(ledger) {
				t.Fatalf("capacity %d step %d: length mismatch", capacity, step)
			}
			e := h.order.Front()
			for _, want := range ledger {
				if e == nil || h.index[want.id] != e {
					t.Fatalf("capacity %d step %d: ordering mismatch", capacity, step)
				}
				e = e.Next()
			}
			if e != nil {
				t.Fatal("unexpected trailing entry")
			}
		}
	}
}
