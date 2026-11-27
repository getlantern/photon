package recovery

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"
)

// A slice ledger is independent of the queue representation. Check FIFO
// eviction and arbitrary removal through recovery's real retention path.
func TestACKOnlyRetentionModel(t *testing.T) {
	for _, capacity := range []int{1, 2, 17} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			r, _ := NewSent(capacity)
			rng := rand.New(rand.NewSource(19))
			var model []uint64
			next := uint64(0)
			for step := 0; step < 10000; step++ {
				switch choice := rng.Intn(10); {
				case choice < 7:
					if err := r.OnSent(Application, Packet{Number: next, Bytes: 40, SentAt: time.Unix(100, 0)}); err != nil {
						t.Fatal(step, err)
					}
					if len(model) == capacity {
						model = model[1:]
					}
					model = append(model, next)
					next++
				case choice < 9 && len(model) > 0:
					index := rng.Intn(len(model))
					pn := model[index]
					r.remove(&r.spaces[Application], r.spaces[Application].packets[pn])
					model = slices.Delete(model, index, index+1)
				default:
					r.Discard(Application)
					model = nil
				}
				s := &r.spaces[Application]
				if len(s.packets) != len(model) || len(s.ackNodes) != len(model) || s.ackOnly.Len() != len(model) || r.BytesInFlight != 0 {
					t.Fatalf("step %d: retention mismatch", step)
				}
				for _, pn := range model {
					if _, ok := s.packets[pn]; !ok || s.ackNodes[pn] == nil {
						t.Fatalf("step %d: packet %d missing", step, pn)
					}
				}
			}
		})
	}
}

func BenchmarkACKOnlyRetention(b *testing.B) {
	for _, capacity := range []int{32, 1024} {
		b.Run(fmt.Sprint(capacity), func(b *testing.B) {
			r, _ := NewSent(capacity)
			now := time.Unix(100, 0)
			// Populate both bounded histories before measuring steady eviction.
			next := uint64(max(capacity*6, 512))
			for pn := uint64(0); pn < next; pn++ {
				if err := r.OnSent(Application, Packet{Number: pn, SentAt: now, Bytes: 40}); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := r.OnSent(Application, Packet{Number: next + uint64(i), SentAt: now, Bytes: 40}); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if len(r.spaces[Application].packets) != capacity || r.spaces[Application].ackOnly.Len() != capacity {
				b.Fatal("retention size changed")
			}
		})
	}
}
