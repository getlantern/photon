package recovery

import (
	"testing"
	"time"
)

// The oracle enumerates endpoint pairs rather than keeping the implementation's
// running interval. It includes unresolved / acknowledged barriers, missing RTT
// samples, clock regressions, non-eliciting packets and MTU probes.
func FuzzPersistentCongestionHistory(f *testing.F) {
	f.Add([]byte{3, 0, 0, 13, 0, 1, 13, 0, 2, 13, 1, 0, 0, 1, 1, 0, 1, 2, 0, 4, 0, 0})
	f.Add([]byte{1, 0, 0, 13, 0, 0, 13, 1, 0, 0, 4, 0, 0})
	// Fill and overflow the largest history, then mark every retained packet
	// lost, exercise a late ACK barrier and consume a surviving loss interval.
	seed := []byte{31}
	for i := 0; i < 64; i++ {
		seed = append(seed, 0, byte(i%3), 13)
	}
	for i := 0; i < 32; i++ {
		seed = append(seed, 1, byte(i), 0)
	}
	seed = append(seed, 2, 16, 0, 4, 0, 40, 4, 0, 0)
	f.Add(seed)
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 {
			return
		}
		if len(data) > 4096 {
			data = data[:4096]
		}
		capacity := 1 + int(data[0]%32)
		h := congestionHistory{capacity: capacity}
		type record struct {
			id                transmissionID
			at                time.Time
			sampled, endpoint bool
			state             uint8
		}
		var ledger []record
		trim := func() {
			for len(ledger) > 0 && ledger[0].state == 2 {
				ledger = ledger[1:]
			}
		}
		now := time.Unix(100, 0)
		for pos := 1; pos+2 < len(data); pos += 3 {
			op, a, b := data[pos]%6, data[pos+1], data[pos+2]
			switch op {
			case 0:
				now = now.Add(time.Duration(int8(b)) * time.Millisecond)
				id := transmissionID{Space(a % 3), uint64(pos)}
				p := Packet{Number: id.number, SentAt: now, AckEliciting: b&1 != 0, MTUProbe: b&2 != 0}
				sampled := b&4 != 0
				h.sent(id.space, p, sampled)
				if len(ledger) == capacity {
					ledger = ledger[1:]
				}
				ledger = append(ledger, record{id: id, at: now, sampled: sampled, endpoint: p.AckEliciting && !p.MTUProbe})
			case 1:
				if len(ledger) > 0 {
					i := int(a) % len(ledger)
					if ledger[i].state != 2 {
						h.lost(ledger[i].id.space, []Packet{{Number: ledger[i].id.number}})
						ledger[i].state = 1
					}
				}
			case 2:
				if len(ledger) > 0 {
					i := int(a) % len(ledger)
					id := ledger[i].id
					h.ack(id.space, ack(id.number, id.number))
					ledger[i].state = 2
					trim()
				}
			case 3:
				space := Space(a % 3)
				h.discard(space)
				for i := range ledger {
					if ledger[i].id.space == space {
						ledger[i].state = 2
					}
				}
				trim()
			case 4:
				duration := time.Duration(b) * time.Millisecond
				through := -1
				for i, p := range ledger {
					if p.state != 1 || !p.sampled || !p.endpoint {
						continue
					}
					for j := i + 1; j < len(ledger); j++ {
						q := ledger[j]
						if q.state != 1 || !q.sampled || q.at.Before(ledger[j-1].at) {
							break
						}
						if q.endpoint && q.at.Sub(p.at) > duration && j > through {
							through = j
						}
					}
				}
				if got := h.detect(duration); got != (through >= 0) {
					t.Fatalf("step %d: detection=%v oracle endpoint=%d ledger=%+v", pos, got, through, ledger)
				}
				if through >= 0 {
					ledger = ledger[through+1:]
				}
			case 5:
				if len(ledger) > 0 {
					i := int(a) % len(ledger)
					id := ledger[i].id
					h.order.Remove(h.index[id])
					delete(h.index, id)
					ledger = append(ledger[:i], ledger[i+1:]...)
				}
			}
			if h.order.Len() != len(ledger) || len(h.index) != len(ledger) {
				t.Fatalf("step %d: length mismatch", pos)
			}
			e := h.order.Front()
			for _, p := range ledger {
				if e == nil || e != h.index[p.id] {
					t.Fatalf("step %d: history order mismatch", pos)
				}
				e = e.Next()
			}
			if e != nil {
				t.Fatalf("step %d: trailing history", pos)
			}
		}
	})
}
