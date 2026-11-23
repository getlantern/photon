package stream

import (
	"io"
	"testing"
)

var receiveAllocationSink *Receive

func BenchmarkReceiveAllocation(b *testing.B) {
	for _, mode := range []string{"idle", "first-byte", "empty-fin", "reset"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				flow := &Flow{limit: 65536}
				r, err := NewReceive(flow, 65536, 65536)
				if err != nil {
					b.Fatal(err)
				}
				switch mode {
				case "first-byte":
					err = r.Push(0, []byte{1}, false)
				case "empty-fin":
					err = r.Push(0, nil, true)
				case "reset":
					err = r.Reset(0, 7)
				}
				if err != nil {
					b.Fatal(err)
				}
				receiveAllocationSink = r
			}
		})
	}
}

func TestReceiveLazyStorage(t *testing.T) {
	flow := &Flow{limit: 128}
	r, err := NewReceive(flow, 64, 32)
	if err != nil {
		t.Fatal(err)
	}
	empty := func() {
		t.Helper()
		if r.data != nil || r.present != nil {
			t.Fatal("unexpected payload allocation")
		}
	}
	empty()
	if _, err := r.Read(make([]byte, 1)); err != ErrWouldBlock {
		t.Fatal(err)
	}
	if err := r.SetLimit(64); err != nil {
		t.Fatal(err)
	}
	if err := r.SetLimit(65); err != ErrReceiveWindow {
		t.Fatal(err)
	}
	if err := r.Push(64, []byte{1}, false); err != ErrFlowControl {
		t.Fatal(err)
	}
	// Empty frames still charge their highest offset without storing data.
	if err := r.Push(32, nil, false); err != nil {
		t.Fatal(err)
	}
	if flow.Charged() != 32 {
		t.Fatal("empty frame accounting")
	}
	empty()
	if err := r.Push(0, []byte("abc"), false); err != nil {
		t.Fatal(err)
	}
	if r.data == nil {
		t.Fatal("missing payload storage")
	}
	// A reset must free existing storage and account missing bytes exactly once.
	if err := r.Reset(40, 7); err != nil {
		t.Fatal(err)
	}
	empty()
	if flow.Charged() != 40 || flow.Consumed() != 40 {
		t.Fatal("reset accounting")
	}
	if err := r.Push(0, []byte("abc"), false); err != nil {
		t.Fatal(err)
	}
	if err := r.Reset(40, 8); err != nil {
		t.Fatal(err)
	}
	empty()
	if flow.Consumed() != 40 || r.reset.Code != 7 {
		t.Fatal("duplicate reset changed accounting or code")
	}
}

func TestReceiveTerminalStorageRelease(t *testing.T) {
	for _, mode := range []string{"empty", "read", "discard"} {
		t.Run(mode, func(t *testing.T) {
			f := &Flow{limit: 64}
			r, _ := NewReceive(f, 64, 64)
			var data []byte
			if mode != "empty" {
				data = []byte("terminal")
			}
			if err := r.Push(0, data, true); err != nil {
				t.Fatal(err)
			}
			if mode == "discard" {
				if err := r.DiscardComplete(); err != nil {
					t.Fatal(err)
				}
			} else {
				got := make([]byte, 64)
				n, err := r.Read(got)
				if err != io.EOF || string(got[:n]) != string(data) {
					t.Fatalf("read %q: %v", got[:n], err)
				}
			}
			if r.data != nil || r.present != nil || !r.Drained() || f.Consumed() != uint64(len(data)) {
				t.Fatal("terminal storage or accounting")
			}
			// Duplicates and terminal reads must not recreate the released arrays.
			if err := r.Push(0, data, true); err != nil {
				t.Fatal(err)
			}
			if _, err := r.Read(make([]byte, 1)); err != io.EOF {
				t.Fatal(err)
			}
			if r.data != nil || r.present != nil {
				t.Fatal("terminal storage recreated")
			}
		})
	}
}
