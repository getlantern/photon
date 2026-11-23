package stream

import (
	"testing"
)

func FuzzReceive(f *testing.F) {
	f.Add([]byte{0, 0, 8, 0, 8, 8, 1, 0, 0, 0, 16, 8, 2, 24, 0})
	f.Add([]byte{0, 16, 8, 0, 0, 8, 0, 8, 8, 1, 0, 0, 3, 24, 0})
	f.Add([]byte{4, 16, 8, 0, 4, 4, 0, 0, 4, 1, 16, 0})
	f.Add([]byte{0, 0, 16, 4, 16, 12, 1, 4, 0, 4, 16, 6, 1, 16, 0})
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 4096 {
			return
		}
		flow, _ := NewFlow(1 << 20)
		capacity := 1 + len(input)%129
		r, _ := NewReceive(flow, capacity, uint64(capacity))
		for i := 0; i+2 < len(input); i += 3 {
			beforeHigh, beforeCharged, beforeConsumed := r.Highest(), flow.Charged(), flow.Consumed()
			var err error
			switch input[i] % 5 {
			case 0, 2:
				offset := r.Offset() + uint64(input[i+1])
				data := make([]byte, int(input[i+2]%32))
				for j := range data {
					data[j] = byte(offset + uint64(j))
				}
				err = r.Push(offset, data, input[i]%5 == 2)
			case 1:
				dst := make([]byte, int(input[i+1]%65))
				start := r.Offset()
				n, _ := r.Read(dst)
				for j := 0; j < n; j++ {
					if dst[j] != byte(start+uint64(j)) {
						t.Fatal("reassembly mismatch")
					}
				}
				if err := r.SetLimit(r.Offset() + uint64(capacity)); err != nil {
					t.Fatal(err)
				}
			case 4:
				err = r.ResetAt(uint64(input[i+1]), uint64(input[i+2]), 7)
			case 3:
				err = r.Reset(uint64(input[i+1]), uint64(input[i+2]))
			}
			if err != nil && (r.Highest() != beforeHigh || flow.Charged() != beforeCharged || flow.Consumed() != beforeConsumed) {
				t.Fatal("failed receive mutated flow state")
			}
			if flow.Charged() != r.Highest() || flow.Consumed() != r.Offset() || flow.Consumed() > flow.Charged() || flow.Charged() > flow.Limit() {
				t.Fatal("flow accounting invariant")
			}
			if final, known := r.FinalSize(); known && r.Highest() > final {
				t.Fatal("data beyond final size")
			}
		}
	})
}
