package stream

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"slices"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

// The expected prefix comes from the absolute-offset oracle, never ring state.
// Snapshot all storage and accounting to catch reads that accidentally consume
// or retire data, even when the returned bytes happen to match.
func assertPeekModel(t *testing.T, r *Receive, prefix []byte, size int, terminal error) {
	t.Helper()
	before := *r
	before.data = bytes.Clone(r.data)
	before.present = slices.Clone(r.present)
	beforeFlow := *r.flow
	dst := bytes.Repeat([]byte{0xa5}, size)
	expected := bytes.Clone(dst)
	count := min(size, len(prefix))
	wantErr := error(nil)
	if count < size {
		if terminal == nil {
			count = 0
			wantErr = ErrWouldBlock
		} else {
			wantErr = terminal
		}
	}
	copy(expected, prefix[:count])
	n, err := r.Peek(dst)
	sameErr := err == wantErr
	if reset, ok := wantErr.(*ResetError); ok {
		var actual *ResetError
		sameErr = errors.As(err, &actual) && actual.Code == reset.Code
	}
	if n != count || !sameErr || !bytes.Equal(dst, expected) {
		t.Fatalf("peek: n=%d err=%v, want n=%d err=%v", n, err, count, wantErr)
	}
	if !reflect.DeepEqual(*r, before) || *r.flow != beforeFlow {
		t.Fatal("peek mutated receive state")
	}
}

func TestReceiveStorageExpansionNearMaximumOffset(t *testing.T) {
	flow, _ := NewFlow(wire.MaxVarint)
	r, _ := NewReceive(flow, 8193, 8193)
	start := wire.MaxVarint - 8193
	// Model an already consumed stream without processing exabytes of input.
	r.next, r.highest, r.limit = start, start, wire.MaxVarint
	flow.charged, flow.consumed = start, start
	if err := r.Push(start+4000, []byte{7}, false); err != nil {
		t.Fatal(err)
	}
	if len(r.data) != 4096 {
		t.Fatal("initial ring size")
	}
	if err := r.Push(start+8000, []byte{9}, false); err != nil {
		t.Fatal(err)
	}
	if len(r.data) != 8192 {
		t.Fatal("missing physical expansion")
	}
	if err := r.Push(start+4000, []byte{8}, false); err != ErrDataConflict {
		t.Fatal("lost high-offset overlap", err)
	}
	want := make([]byte, 8193)
	want[4000], want[8000] = 7, 9
	if err := r.Push(start, want, true); err != nil {
		t.Fatal(err)
	}
	assertPeekModel(t, r, want, len(want), io.EOF)
	assertPeekModel(t, r, want, len(want)+1, io.EOF)
	out := make([]byte, len(want))
	if n, err := r.Read(out); n != len(want) || err != io.EOF || !bytes.Equal(out, want) {
		t.Fatal(n, err)
	}
	if flow.Consumed() != wire.MaxVarint || flow.Charged() != wire.MaxVarint {
		t.Fatal("high-offset accounting")
	}
}

// The oracle stores bytes by absolute offset, independently of ring positions,
// bitmaps, growth/remapping and the implementation's highest-offset state.
func FuzzReceiveStorageModel(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{128, 0, 0, 10, 0, 3, 255, 255, 0, 0, 0, 0, 32, 63, 0})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 31, 63, 0, 3, 255, 255, 0, 0, 0, 255, 63, 10, 0, 2, 0, 16, 0, 0})
	var wrapped []byte
	for i := 0; i < 80; i++ {
		wrapped = append(wrapped, 1, 0, 0, 127, 0, 2, 128, 0, 0, 0)
	}
	f.Add(wrapped)
	var wrappedPeek []byte
	for i := 0; i < 80; i++ {
		// A 127-byte advance crosses bitmap and physical-ring boundaries;
		// argument 37 requests a nonempty peek before the consuming read.
		wrappedPeek = append(wrappedPeek, 1, 37, 0, 126, 0, 2, 128, 0, 0, 0)
	}
	f.Add(wrappedPeek)
	f.Fuzz(func(t *testing.T, actions []byte) {
		if len(actions) > 1280 {
			actions = actions[:1280]
		}
		flow, _ := NewFlow(wire.MaxVarint)
		capacity := 4097
		r, _ := NewReceive(flow, capacity, uint64(capacity))
		stored := make(map[uint64]byte)
		var next, highest uint64
		value := func(offset uint64) byte { return byte(offset) ^ byte(offset>>8) }
		for i := 0; i+4 < len(actions); i += 5 {
			a := actions[i : i+5]
			argument := uint64(a[1]) | uint64(a[2])<<8
			switch a[0] % 5 {
			case 0, 1, 4:
				offset := next + argument%uint64(capacity+1024)
				if a[0]%5 == 1 {
					offset = next
				}
				data := make([]byte, 1+int(a[3]))
				conflict := false
				for j := range data {
					at := offset + uint64(j)
					data[j] = value(at)
					if a[0]%5 == 4 {
						data[j] ^= 1
					}
					if old, ok := stored[at]; ok && old != data[j] {
						conflict = true
					}
				}
				end := offset + uint64(len(data))
				var want error
				if end > next+uint64(capacity) {
					want = ErrFlowControl
				} else if conflict {
					want = ErrDataConflict
				}
				if err := r.Push(offset, data, false); err != want {
					t.Fatalf("push: %v, want %v", err, want)
				}
				if want == nil {
					highest = max(highest, end)
					for j, b := range data {
						stored[offset+uint64(j)] = b
					}
				}
			case 2:
				out := make([]byte, 1+int(argument%8193))
				want := make([]byte, 0, len(out))
				for len(want) < len(out) {
					b, ok := stored[next+uint64(len(want))]
					if !ok {
						break
					}
					want = append(want, b)
				}
				n, err := r.Read(out)
				expectedErr := error(nil)
				if len(want) == 0 {
					expectedErr = ErrWouldBlock
				}
				if n != len(want) || !bytes.Equal(out[:n], want) || err != expectedErr {
					t.Fatalf("read: n=%d err=%v, want n=%d err=%v", n, err, len(want), expectedErr)
				}
				for j := 0; j < n; j++ {
					delete(stored, next+uint64(j))
				}
				next += uint64(n)
			case 3:
				capacity = min(32771, capacity+1+int(argument))
				if err := r.Grow(capacity); err != nil {
					t.Fatal(err)
				}
			}
			if err := r.SetLimit(next + uint64(capacity)); err != nil {
				t.Fatal(err)
			}
			if r.Offset() != next || r.Highest() != highest || flow.Consumed() != next || flow.Charged() != highest || r.buffered != uint64(len(stored)) {
				t.Fatal("oracle state mismatch")
			}
			if len(r.data) > capacity || len(r.present) != (len(r.data)+63)/64 {
				t.Fatal("storage bound")
			}
			size := int(argument % 8194)
			prefix := make([]byte, 0, size)
			for len(prefix) < size {
				b, ok := stored[next+uint64(len(prefix))]
				if !ok {
					break
				}
				prefix = append(prefix, b)
			}
			assertPeekModel(t, r, prefix, size, nil)

		}
		target := highest
		partial := len(actions) > 0 && actions[0]&128 != 0
		if partial {
			target = next + (highest-next)/2
			if err := r.ResetAt(highest, target, 7); err != nil {
				t.Fatal(err)
			}
		}
		// Fill every gap with oracle bytes and verify a single terminal read. This
		// detects corruption of out-of-order bytes even if random reads missed them.
		want := make([]byte, int(target-next))
		for j := range want {
			at := next + uint64(j)
			b, ok := stored[at]
			if !ok {
				b = value(at)
			}
			want[j] = b
		}
		if err := r.Push(next, want, false); err != nil {
			t.Fatal(err)
		}
		if !partial {
			if err := r.Push(highest, nil, true); err != nil {
				t.Fatal(err)
			}
		}
		terminal := error(io.EOF)
		if partial {
			terminal = &ResetError{Code: 7}
		}
		for j := 0; j < 2; j++ {
			assertPeekModel(t, r, want, len(want), terminal)
			assertPeekModel(t, r, want, len(want)+1, terminal)
		}
		out := make([]byte, len(want)+1)
		n, err := r.Read(out)
		if partial {
			var reset *ResetError
			if !errors.As(err, &reset) || reset.Code != 7 {
				t.Fatal("missing partial reset", err)
			}
		} else if err != io.EOF {
			t.Fatal("missing EOF", err)
		}
		if n != len(want) || !bytes.Equal(out[:n], want) || flow.Consumed() != highest || flow.Charged() != highest || r.data != nil || r.present != nil {
			t.Fatal("terminal oracle mismatch")
		}
	})
}
