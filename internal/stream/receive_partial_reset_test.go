package stream

import (
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func assertPartialRead(t *testing.T, r *Receive, size int, want string, reset bool) {
	t.Helper()
	dst := make([]byte, size)
	n, err := r.Read(dst)
	var failure *ResetError
	if string(dst[:n]) != want || (reset && (!errors.As(err, &failure) || failure.Code != 7)) || (!reset && err != nil) {
		t.Fatalf("read %q, %v; want %q reset=%t", dst[:n], err, want, reset)
	}
}

func TestReceivePartialResetGapAndGrowth(t *testing.T) {
	r, flow := receiver(t, 16, 64)
	if err := r.ResetAt(10, 6, 7); err != nil {
		t.Fatal(err)
	}
	if err := r.Push(3, []byte("defTAIL"), false); err != nil {
		t.Fatal(err)
	}
	if r.Complete() || r.Drained() || flow.Charged() != 10 || flow.Consumed() != 0 {
		t.Fatal("premature reset completion")
	}
	if _, err := r.Read(make([]byte, 10)); !errors.Is(err, ErrWouldBlock) {
		t.Fatal("gap surfaced reset", err)
	}
	if err := r.Push(0, []byte("abc"), false); err != nil {
		t.Fatal(err)
	}
	if !r.Complete() || r.Drained() {
		t.Fatal("complete prefix must remain readable")
	}
	assertPartialRead(t, r, 2, "ab", false)
	if err := r.Grow(32); err != nil {
		t.Fatal(err)
	}
	assertPartialRead(t, r, 20, "cdef", true)
	if !r.Drained() || flow.Consumed() != 10 || r.Offset() != 10 || r.data != nil {
		t.Fatal("reset retirement accounting")
	}
	if err := r.ResetAt(10, 9, 7); err != nil {
		t.Fatal(err)
	}
	if err := r.Push(0, []byte("discarded!"), true); err != nil {
		t.Fatal(err)
	}
	if flow.Charged() != 10 || flow.Consumed() != 10 {
		t.Fatal("duplicate reset charged twice")
	}
}

func TestReceivePartialResetReductionAndOrdinaryReset(t *testing.T) {
	for _, ordinary := range []bool{false, true} {
		r, flow := receiver(t, 16, 64)
		if err := r.Push(0, []byte("abcdefgh"), false); err != nil {
			t.Fatal(err)
		}
		if err := r.ResetAt(12, 8, 7); err != nil {
			t.Fatal(err)
		}
		assertPartialRead(t, r, 2, "ab", false)
		if ordinary {
			if err := r.Reset(12, 7); err != nil {
				t.Fatal(err)
			}
			assertPartialRead(t, r, 16, "", true)
		} else {
			if err := r.ResetAt(12, 4, 7); err != nil {
				t.Fatal(err)
			}
			if err := r.ResetAt(12, 8, 7); err != nil {
				t.Fatal(err)
			}
			assertPartialRead(t, r, 16, "cd", true)
		}
		if flow.Consumed() != 12 {
			t.Fatal("reduced reset accounting")
		}
		if err := r.Reset(12, 8); !errors.Is(err, ErrStreamState) {
			t.Fatal("changed partial-reset error code", err)
		}
	}
	r, _ := receiver(t, 16, 64)
	if err := r.Reset(12, 7); err != nil {
		t.Fatal(err)
	}
	if err := r.ResetAt(12, 8, 7); err != nil {
		t.Fatal(err)
	}
	assertPartialRead(t, r, 16, "", true)
}

func TestReceivePartialResetValidationAtomic(t *testing.T) {
	r, flow := receiver(t, 16, 10)
	if err := r.Push(0, []byte("ab"), false); err != nil {
		t.Fatal(err)
	}
	if err := r.ResetAt(8, 4, 7); err != nil {
		t.Fatal(err)
	}
	before, beforeFlow := *r, *flow
	for _, tc := range []struct {
		final, reliable, code uint64
		want                  error
	}{
		{8, 9, 7, wire.ErrFrame}, {9, 4, 7, ErrFinalSize}, {8, 4, 8, ErrStreamState}, {8, 4, wire.MaxVarint + 1, ErrFinalSize},
	} {
		if err := r.ResetAt(tc.final, tc.reliable, tc.code); !errors.Is(err, tc.want) {
			t.Fatal(tc, err)
		}
		if !reflect.DeepEqual(*r, before) || *flow != beforeFlow {
			t.Fatal("invalid reset mutated state")
		}
	}
	other, _ := NewReceive(flow, 16, 16)
	if err := other.ResetAt(3, 1, 7); !errors.Is(err, ErrFlowControl) || flow.Charged() != 8 {
		t.Fatal("connection credit bypass", err)
	}
	if err := r.Push(8, []byte("!"), false); !errors.Is(err, ErrFinalSize) {
		t.Fatal("late final-size violation", err)
	}
}

func TestReceivePartialResetFINAndWrap(t *testing.T) {
	for _, readFIN := range []bool{false, true} {
		r, _ := receiver(t, 8, 64)
		if err := r.Push(0, []byte("abcd"), true); err != nil {
			t.Fatal(err)
		}
		if readFIN {
			if _, err := r.Read(make([]byte, 8)); err != io.EOF {
				t.Fatal(err)
			}
		}
		if err := r.ResetAt(4, 4, 7); err != nil {
			t.Fatal(err)
		}
		if readFIN {
			if _, err := r.Read(make([]byte, 8)); err != io.EOF {
				t.Fatal("reset replaced delivered EOF", err)
			}
		} else {
			assertPartialRead(t, r, 8, "abcd", true)
		}
		if err := r.ResetAt(4, 2, 8); !errors.Is(err, ErrStreamState) {
			t.Fatal("error change after terminal delivery", err)
		}
	}
	r, flow := receiver(t, 8, 64)
	if err := r.Push(0, []byte("abcdefgh"), false); err != nil {
		t.Fatal(err)
	}
	assertPartialRead(t, r, 8, "abcdefgh", false)
	if err := r.SetLimit(16); err != nil {
		t.Fatal(err)
	}
	if err := r.Push(12, []byte("mnop"), false); err != nil {
		t.Fatal(err)
	}
	if err := r.ResetAt(16, 14, 7); err != nil {
		t.Fatal(err)
	}
	if err := r.Push(8, []byte("ijkl"), false); err != nil {
		t.Fatal(err)
	}
	assertPartialRead(t, r, 8, "ijklmn", true)
	if flow.Consumed() != 16 {
		t.Fatal("wrapped accounting")
	}
}
