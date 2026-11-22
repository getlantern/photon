package stream

import (
	"bytes"
	"fmt"
	"testing"
)

func TestReceiveStorageSpanGrowthAndPartialReset(t *testing.T) {
	flow, _ := NewFlow(4 << 20)
	r, _ := NewReceive(flow, 512<<10, 512<<10)
	prefix := bytes.Repeat([]byte{1}, 4089)
	if err := r.Push(0, prefix, false); err != nil {
		t.Fatal(err)
	}
	if len(r.data) != 4096 || len(r.present)*8 != 512 {
		t.Fatal("small span allocated full window")
	}
	if n, err := r.Read(prefix); n != len(prefix) || err != nil {
		t.Fatal(n, err)
	}
	tail := []byte("abcdefghijklmnop")
	if err := r.Push(4091, tail, false); err != nil {
		t.Fatal(err)
	}
	if err := r.Push(10089, []byte{9}, false); err != nil {
		t.Fatal(err)
	}
	if len(r.data) != 8192 {
		t.Fatal("span did not grow ring", len(r.data))
	}
	charged := flow.Charged()
	if err := r.Push(4091, []byte("X"), false); err != ErrDataConflict || flow.Charged() != charged {
		t.Fatal("remapping lost overlap validation", err)
	}
	if err := r.Grow(1 << 20); err != nil || len(r.data) != 8192 || r.Limit() != 512<<10 {
		t.Fatal("credit growth allocated payload or changed limit", err)
	}
	if err := r.ResetAt(11089, 4107, 7); err != nil {
		t.Fatal(err)
	}
	if r.buffered != uint64(len(tail)) {
		t.Fatal("reset failed to discard far tail", r.buffered)
	}
	if err := r.Push(4089, []byte("AB"), false); err != nil {
		t.Fatal(err)
	}
	out := make([]byte, 64)
	n, err := r.Read(out)
	reset, ok := err.(*ResetError)
	if n != 18 || !bytes.Equal(out[:n], append([]byte("AB"), tail...)) || !ok || reset.Code != 7 {
		t.Fatal(n, err, string(out[:n]))
	}
	if r.data != nil || r.present != nil || flow.Consumed() != 11089 {
		t.Fatal("terminal storage/accounting")
	}
}

func TestReceiveStorageClipsToOddWindow(t *testing.T) {
	flow, _ := NewFlow(10000)
	r, _ := NewReceive(flow, 5001, 5001)
	if err := r.Push(0, []byte{1}, false); err != nil {
		t.Fatal(err)
	}
	if err := r.Push(5000, []byte{2}, true); err != nil {
		t.Fatal(err)
	}
	if len(r.data) != 5001 {
		t.Fatal("growth exceeded window", len(r.data))
	}
	if err := r.Push(1, make([]byte, 4999), false); err != nil {
		t.Fatal(err)
	}
	out := make([]byte, 5001)
	n, _ := r.Read(out)
	if n != 5001 || out[0] != 1 || out[5000] != 2 {
		t.Fatal("odd-window remapping", n)
	}
}

func BenchmarkReceiveWindowFirstByte(b *testing.B) {
	for _, capacity := range []int{65536, 512 << 10, 6 << 20} {
		b.Run(fmt.Sprint(capacity), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				flow, _ := NewFlow(uint64(capacity))
				r, _ := NewReceive(flow, capacity, uint64(capacity))
				if err := r.Push(0, []byte{1}, false); err != nil {
					b.Fatal(err)
				}
				receiveAllocationSink = r
			}
		})
	}
}
