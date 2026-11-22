package stream

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestReceiveGrowWrappedGaps(t *testing.T) {
	for _, capacity := range []int{1, 7, 63, 64, 65, 127} {
		flow, _ := NewFlow(4096)
		r, _ := NewReceive(flow, capacity, uint64(capacity))
		prefix := bytes.Repeat([]byte{1}, capacity-1)
		if err := r.Push(0, prefix, false); err != nil {
			t.Fatal(err)
		}
		if n, err := r.Read(prefix); n != capacity-1 || err != nil {
			t.Fatal(n, err)
		}
		if err := r.SetLimit(uint64(2*capacity - 1)); err != nil {
			t.Fatal(err)
		}
		tail := bytes.Repeat([]byte{2}, capacity-1)
		if err := r.Push(uint64(capacity), tail, true); err != nil {
			t.Fatal(err)
		}
		charged, consumed := flow.Charged(), flow.Consumed()
		if err := r.Grow(3*capacity + 1); err != nil {
			t.Fatal(err)
		}
		if flow.Charged() != charged || flow.Consumed() != consumed || r.Limit() != uint64(2*capacity-1) {
			t.Fatal("growth changed credit")
		}
		if len(tail) > 0 {
			if err := r.Push(uint64(capacity), []byte{3}, false); err != ErrDataConflict {
				t.Fatal("growth lost overlap detection", err)
			}
			if flow.Charged() != charged || flow.Consumed() != consumed {
				t.Fatal("conflicting overlap mutated credit")
			}
		}

		if _, err := r.Read(make([]byte, capacity)); err != ErrWouldBlock {
			t.Fatal("growth filled gap", err)
		}
		if err := r.Push(uint64(capacity-1), []byte{2}, false); err != nil {
			t.Fatal(err)
		}
		prefix = make([]byte, capacity)
		if n, err := r.Read(prefix); n != capacity || err != io.EOF || !bytes.Equal(prefix, bytes.Repeat([]byte{2}, capacity)) {
			t.Fatal("grown data", capacity, n, err)
		}
	}
}

func TestReceiveGrowBoundsAndLazy(t *testing.T) {
	flow, _ := NewFlow(wire.MaxVarint)
	r, _ := NewReceive(flow, 65, 65)
	for _, bad := range []int{-1, 0, 64, 16<<20 + 1} {
		if err := r.Grow(bad); err != ErrReceiveWindow || r.capacity != 65 {
			t.Fatal("invalid growth mutated receiver", bad, err)
		}
	}
	if err := r.Grow(16 << 20); err != nil || r.data != nil || r.present != nil || r.Limit() != 65 {
		t.Fatal("idle growth allocated or changed credit", err)
	}
	// Absolute offsets near the wire maximum must not wrap during remapping.
	r, _ = NewReceive(flow, 65, 65)
	r.next = wire.MaxVarint - 64
	r.highest = r.next
	r.limit = wire.MaxVarint
	if err := r.Push(wire.MaxVarint-1, []byte{7}, true); err != nil {
		t.Fatal(err)
	}
	if err := r.Grow(129); err != nil {
		t.Fatal(err)
	}
	index := int((wire.MaxVarint - 1) % uint64(len(r.data)))
	if r.data[index] != 7 || r.present[index/64]&(1<<uint(index%64)) == 0 {
		t.Fatal("high offset remapping")
	}
}

// Compare growing storage with a fixed-capacity receiver using identical
// advertised limits, input, reads and resets. Growth cannot alter semantics.
func FuzzReceiveGrowth(f *testing.F) {
	f.Add([]byte{65, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	f.Add([]byte{1, 2, 255, 0, 8, 16, 32, 64, 128})
	f.Fuzz(func(t *testing.T, actions []byte) {
		if len(actions) == 0 {
			return
		}
		if len(actions) > 512 {
			actions = actions[:512]
		}
		capacity := 1 + int(actions[0])%128
		flow, _ := NewFlow(wire.MaxVarint)
		referenceFlow, _ := NewFlow(wire.MaxVarint)
		r, _ := NewReceive(flow, capacity, uint64(capacity))
		reference, _ := NewReceive(referenceFlow, 512, uint64(capacity))
		for _, action := range actions[1:] {
			var a, b error
			switch action % 5 {
			case 0:
				size := min(512, r.capacity+int(action)+1)
				limit := r.Limit()
				if err := r.Grow(size); err != nil || r.Limit() != limit {
					t.Fatal("growth changed limit", err)
				}
			case 1:
				limit := r.next + uint64(r.capacity)
				a, b = r.SetLimit(limit), reference.SetLimit(limit)
			case 2:
				offset := r.next + uint64(action)%uint64(r.capacity)
				data := []byte{byte(offset)}
				a, b = r.Push(offset, data, action&128 != 0), reference.Push(offset, data, action&128 != 0)
			case 3:
				dst, want := make([]byte, 1+int(action)%32), make([]byte, 1+int(action)%32)
				var n, m int
				n, a = r.Read(dst)
				m, b = reference.Read(want)
				if n != m || !bytes.Equal(dst[:n], want[:m]) {
					t.Fatal("growth changed read")
				}
			case 4:
				end := r.next + uint64(action)%uint64(r.capacity)
				a, b = r.Reset(end, 1), reference.Reset(end, 1)
			}
			if fmt.Sprint(a) != fmt.Sprint(b) || *flow != *referenceFlow || r.next != reference.next || r.highest != reference.highest || r.buffered != reference.buffered || r.final != reference.final || r.finalKnown != reference.finalKnown {
				t.Fatal("growth changed semantics", a, b)
			}
		}
	})
}
