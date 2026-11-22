package stream

import (
	"errors"
	"io"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func receiver(t *testing.T, capacity int, limit uint64) (*Receive, *Flow) {
	t.Helper()
	f, err := NewFlow(limit)
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewReceive(f, capacity, uint64(capacity))
	if err != nil {
		t.Fatal(err)
	}
	return r, f
}
func TestReceiveReorderedFIN(t *testing.T) {
	r, f := receiver(t, 16, 100)
	if err := r.Push(3, []byte("def"), true); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 16)
	if _, err := r.Read(b); !errors.Is(err, ErrWouldBlock) {
		t.Fatal(err)
	}
	if f.Charged() != 6 {
		t.Fatal("gap must consume credit")
	}
	if err := r.Push(0, []byte("abcd"), false); err != nil {
		t.Fatal(err)
	}
	if err := r.Push(1, []byte("bcde"), false); err != nil {
		t.Fatal(err)
	}
	n, err := r.Read(b)
	if n != 6 || err != io.EOF || string(b[:n]) != "abcdef" || f.Consumed() != 6 {
		t.Fatal(n, err, string(b[:n]))
	}
	if err := r.Push(0, []byte("abc"), false); err != nil {
		t.Fatal(err)
	}
	if n, err = r.Read(b); n != 0 || err != io.EOF {
		t.Fatal(n, err)
	}
	if err := r.Reset(6, 42); err != nil {
		t.Fatal(err)
	}
	if _, err = r.Read(b); err != io.EOF {
		t.Fatal("reset changed fully read stream", err)
	}
}
func TestReceiveFinalAndAtomicValidation(t *testing.T) {
	r, f := receiver(t, 16, 100)
	r.Push(5, []byte("xyz"), false)
	for _, fn := range []func() error{
		func() error { return r.Push(0, nil, true) },
		func() error { return r.Reset(7, 0) },
	} {
		if err := fn(); !errors.Is(err, ErrFinalSize) {
			t.Fatal(err)
		}
	}
	if err := r.Push(4, []byte("aXyz"), true); !errors.Is(err, ErrDataConflict) {
		t.Fatal(err)
	}
	if _, ok := r.FinalSize(); ok {
		t.Fatal("rejected frame committed FIN")
	}
	if f.Charged() != 8 {
		t.Fatal("rejected frame changed credit")
	}
	if err := r.Push(8, nil, true); err != nil {
		t.Fatal(err)
	}
	if err := r.Push(8, []byte("!"), false); !errors.Is(err, ErrFinalSize) {
		t.Fatal(err)
	}
	if err := r.Reset(9, 0); !errors.Is(err, ErrFinalSize) {
		t.Fatal(err)
	}
}
func TestReceiveFlowAndReset(t *testing.T) {
	r, f := receiver(t, 16, 20)
	other, _ := NewReceive(f, 16, 16)
	if err := r.Push(8, []byte("abcd"), false); err != nil {
		t.Fatal(err)
	}
	if err := other.Push(8, []byte("!"), false); !errors.Is(err, ErrFlowControl) {
		t.Fatal(err)
	}
	if other.Highest() != 0 || f.Charged() != 12 {
		t.Fatal("failed frame mutated accounting")
	}
	if err := r.Reset(16, 77); err != nil {
		t.Fatal(err)
	}
	if f.Charged() != 16 || f.Consumed() != 16 {
		t.Fatal("reset failed to account missing data")
	}
	if err := r.Reset(16, 88); err != nil {
		t.Fatal(err)
	}
	_, err := r.Read(make([]byte, 1))
	var reset *ResetError
	if !errors.As(err, &reset) || reset.Code != 77 {
		t.Fatal(err)
	}
	if err := r.Push(0, []byte("discarded"), false); err != nil {
		t.Fatal(err)
	}
	if f.Consumed() != 16 {
		t.Fatal("duplicate reset/data consumed twice")
	}
	if err := other.Push(0, []byte("abcd"), true); err != nil {
		t.Fatal(err)
	}
	if f.Charged() != 20 {
		t.Fatal(f.Charged())
	}
}
func TestReceiveSlidingWindowAndCopy(t *testing.T) {
	r, f := receiver(t, 4, 100)
	data := []byte("abcd")
	r.Push(0, data, false)
	data[0] = 'X'
	b := make([]byte, 3)
	n, err := r.Read(b)
	if n != 3 || err != nil || string(b) != "abc" {
		t.Fatal(n, err, string(b))
	}
	if err := r.SetLimit(8); !errors.Is(err, ErrReceiveWindow) {
		t.Fatal("credit exceeds buffer", err)
	}
	if err := r.SetLimit(7); err != nil {
		t.Fatal(err)
	}
	if err := r.Push(4, []byte("efg"), true); err != nil {
		t.Fatal(err)
	}
	b = make([]byte, 4)
	n, err = r.Read(b)
	if n != 4 || err != io.EOF || string(b) != "defg" || f.Consumed() != 7 {
		t.Fatal(n, err, string(b))
	}
}
func TestReceiveExtremeOffsets(t *testing.T) {
	r, f := receiver(t, 8, wire.MaxVarint)
	for _, off := range []uint64{9, wire.MaxVarint, ^uint64(0)} {
		if err := r.Push(off, []byte("x"), false); !errors.Is(err, ErrFlowControl) {
			t.Fatal(off, err)
		}
	}
	if r.Highest() != 0 || f.Charged() != 0 {
		t.Fatal("extreme offsets mutated state")
	}
	if err := r.Push(8, nil, false); err != nil || f.Charged() != 8 {
		t.Fatal("empty frame offset accounting", err)
	}
}
