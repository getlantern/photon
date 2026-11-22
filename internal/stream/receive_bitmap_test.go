package stream

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

func BenchmarkReceiveCycle(b *testing.B) {
	for _, size := range []int{64, 1200, 16384} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			f := &Flow{limit: 65536}
			r, _ := NewReceive(f, 65536, 65536)
			payload := make([]byte, size)
			dst := make([]byte, size)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for b.Loop() {
				if err := r.Push(r.Offset(), payload, false); err != nil {
					b.Fatal(err)
				}
				if n, err := r.Read(dst); err != nil || n != size {
					b.Fatalf("read %d: %v", n, err)
				}
				if err := r.SetLimit(r.Offset() + 65536); err != nil {
					b.Fatal(err)
				}
				if err := f.SetLimit(r.Offset() + 65536); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestReceiveOccupancyAcrossWrap(t *testing.T) {
	for _, capacity := range []int{1, 7, 63, 64, 65, 127, 128, 129} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			f := &Flow{limit: 1 << 20}
			r, _ := NewReceive(f, capacity, uint64(capacity))
			for round := 0; round < 10; round++ {
				offset := r.Offset()
				want := make([]byte, capacity)
				for i := range want {
					want[i] = byte(i + round)
				}
				// Reverse byte delivery leaves a gap while occupying neighboring bits.
				for i := capacity - 1; i >= 0; i-- {
					if err := r.Push(offset+uint64(i), want[i:i+1], false); err != nil {
						t.Fatal(err)
					}
					if i > 0 {
						if _, err := r.Read(make([]byte, 1)); err != ErrWouldBlock {
							t.Fatalf("gap %d: %v", i, err)
						}
					}
				}
				bad := bytes.Clone(want)
				bad[len(bad)-1] ^= 1
				if err := r.Push(offset, bad, true); err != ErrDataConflict {
					t.Fatalf("conflicting overlap: %v", err)
				}
				if _, known := r.FinalSize(); known {
					t.Fatal("failed overlap committed FIN")
				}
				if err := r.Push(offset, want, false); err != nil {
					t.Fatal(err)
				}
				// Partial consumption rotates each later round across word/ring boundaries.
				n := max(1, capacity/2)
				got := make([]byte, n)
				if count, err := r.Read(got); err != nil || count != n || !bytes.Equal(got, want[:n]) {
					t.Fatalf("read: %d %v", count, err)
				}
				if err := r.SetLimit(r.Offset() + uint64(capacity)); err != nil {
					t.Fatal(err)
				}
				// Consume the rest before the next round, then add a one-byte rotation.
				if capacity > n {
					got = make([]byte, capacity-n)
					if count, err := r.Read(got); err != nil || count != len(got) || !bytes.Equal(got, want[n:]) {
						t.Fatalf("tail: %d %v", count, err)
					}
				}
				if err := r.SetLimit(r.Offset() + uint64(capacity)); err != nil {
					t.Fatal(err)
				}
				if err := r.Push(r.Offset(), []byte{9}, false); err != nil {
					t.Fatal(err)
				}
				if n, err := r.Read(make([]byte, 1)); n != 1 || err != nil {
					t.Fatal(n, err)
				}
				if err := r.SetLimit(r.Offset() + uint64(capacity)); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestReceiveOverlapFailureLeavesGaps(t *testing.T) {
	f := &Flow{limit: 256}
	r, _ := NewReceive(f, 129, 129)
	if err := r.Push(128, []byte{7}, false); err != nil {
		t.Fatal(err)
	}
	bad := make([]byte, 129)
	if err := r.Push(0, bad, true); err != ErrDataConflict {
		t.Fatal(err)
	}
	if r.buffered != 1 || f.Charged() != 129 || r.Complete() {
		t.Fatal("rejected overlap committed data or FIN")
	}
	if _, err := r.Read(make([]byte, 129)); err != ErrWouldBlock {
		t.Fatal("rejected prefix filled gap", err)
	}
	bad[128] = 7
	if err := r.Push(0, bad, true); err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, 129)
	if n, err := r.Read(dst); n != 129 || err != io.EOF || !bytes.Equal(dst, bad) {
		t.Fatal(n, err)
	}
}
