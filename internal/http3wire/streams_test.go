package http3wire

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"

	"github.com/getlantern/photon/internal/wire"
)

func TestUniTypePrefix(t *testing.T) {
	for _, typ := range []uint64{0, 1, 2, 3, 33, wire.MaxVarint} {
		b, _ := wire.AppendVarint(nil, typ)
		r := iotest.OneByteReader(bytes.NewReader(append(b, 99)))
		got, err := ReadUniType(r)
		if err != nil || got != typ {
			t.Fatalf("%d: %v", got, err)
		}
		var rest [1]byte
		if _, err := io.ReadFull(r, rest[:]); err != nil || rest[0] != 99 {
			t.Fatal("prefix consumed payload")
		}
	}
	if typ, err := ReadUniType(bytes.NewReader([]byte{0x40, 2})); err != nil || typ != 2 {
		t.Fatalf("nonminimal: %d %v", typ, err)
	}
	for _, b := range [][]byte{nil, {0x40}, {0xc0, 0, 0}} {
		_, err := ReadUniType(bytes.NewReader(b))
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("pretype closure: %v", err)
		}
		var protocol *Error
		if errors.As(err, &protocol) {
			t.Fatal("pretype closure became connection error")
		}
	}
	reset := errors.New("reset before type")
	if _, err := ReadUniType(brokenReader{reset}); err != reset {
		t.Fatal(err)
	}
}
func TestCriticalStreamOwnership(t *testing.T) {
	for _, server := range []bool{false, true} {
		r := NewUniRegistry(server)
		first := uint64(2)
		if server {
			first = 3
		}
		for i, typ := range []uint64{UniControl, UniQPACKEncoder, UniQPACKDecoder} {
			id := first + 4*uint64(i)
			if known, err := r.Register(id, typ); err != nil || !known {
				t.Fatalf("%t: %v", known, err)
			}
			_, err := r.Register(id+100, typ)
			requireCode(t, err, StreamCreationError)
			requireCode(t, r.Closed(id), ClosedCriticalStream)
			_, err = r.Register(id+200, typ)
			requireCode(t, err, StreamCreationError)
		}
		if err := r.Closed(first + 400); err != nil {
			t.Fatal(err)
		}
		for i := uint64(0); i < 1000; i++ {
			if known, err := r.Register(first+400+4*i, 33+31*i); err != nil || known {
				t.Fatal(known, err)
			}
		}
	}
}
func TestUniStreamRolesAndRollback(t *testing.T) {
	r := NewUniRegistry(false)
	for _, id := range []uint64{0, 1, 3, wire.MaxVarint + 1} {
		_, err := r.Register(id, UniControl)
		requireCode(t, err, StreamCreationError)
	}
	_, err := r.Register(2, UniPush)
	requireCode(t, err, StreamCreationError)
	if known, err := r.Register(2, UniControl); err != nil || !known {
		t.Fatal(known, err)
	}
	_, err = r.Register(2, UniQPACKEncoder)
	requireCode(t, err, StreamCreationError)
	if known, err := r.Register(6, UniQPACKEncoder); err != nil || !known {
		t.Fatal(known, err)
	}
	server := NewUniRegistry(true)
	for _, id := range []uint64{3, 7} {
		if known, err := server.Register(id, UniPush); err != nil || !known {
			t.Fatal(known, err)
		}
	}
}
func TestConcurrentCriticalStreamRegistration(t *testing.T) {
	r := NewUniRegistry(true)
	var success atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			known, err := r.Register(uint64(i)*4+3, UniControl)
			if err == nil {
				if !known {
					t.Error("unknown control")
				}
				success.Add(1)
			} else {
				var protocol *Error
				if !errors.As(err, &protocol) || protocol.Code != StreamCreationError {
					t.Error(err)
				}
			}
		}(i)
	}
	wg.Wait()
	if success.Load() != 1 {
		t.Fatalf("owners=%d", success.Load())
	}
}
