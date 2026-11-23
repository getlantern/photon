package handshake

import (
	"bytes"
	"errors"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestReassembly(t *testing.T) {
	r := newReassembler(16)
	if err := r.push(5, []byte("world")); err != nil {
		t.Fatal(err)
	}
	if b := r.pop(); len(b) != 0 {
		t.Fatal("released data past gap")
	}
	if err := r.push(7, []byte("rld")); err != nil {
		t.Fatal("duplicate", err)
	}
	if err := r.push(4, []byte("!BAD")); !errors.Is(err, ErrCryptoConflict) {
		t.Fatal(err)
	}
	if r.present[4] {
		t.Fatal("failed insertion changed state")
	}
	if err := r.push(0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if b := r.pop(); string(b) != "helloworld" {
		t.Fatalf("%q", b)
	}
	if err := r.push(0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if len(r.pop()) != 0 {
		t.Fatal("duplicate delivered twice")
	}
	if err := r.push(8, []byte("ld0123456789abcdef")); err != nil {
		t.Fatal(err)
	}
	if b := r.pop(); string(b) != "0123456789abcdef" {
		t.Fatalf("wrap: %q", b)
	}
	if err := r.push(1000000000000, []byte{1}); !errors.Is(err, ErrCryptoBuffer) {
		t.Fatal(err)
	}
	if err := r.push(wire.MaxVarint, []byte{1}); !errors.Is(err, wire.ErrVarintRange) {
		t.Fatal(err)
	}
	if err := r.push(wire.MaxVarint, nil); err != nil {
		t.Fatal("empty data", err)
	}
}

func FuzzReassembly(f *testing.F) {
	f.Add([]byte("hello world"), uint8(3))
	f.Fuzz(func(t *testing.T, data []byte, chunk uint8) {
		if len(data) > 4096 {
			return
		}
		r := newReassembler(4096)
		size := int(chunk) + 1
		for end := len(data); end > 0; {
			start := max(0, end-size)
			if err := r.push(uint64(start), data[start:end]); err != nil {
				t.Fatal(err)
			}
			end = start
		}
		if out := r.pop(); !bytes.Equal(out, data) {
			t.Fatal("out-of-order reassembly")
		}
		if out := r.pop(); len(out) != 0 {
			t.Fatal("delivered twice")
		}
	})
}
