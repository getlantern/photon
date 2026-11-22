package stream

import (
	"errors"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestStreamIDsRolesAndDirections(t *testing.T) {
	for _, server := range []bool{false, true} {
		s, _ := NewIDs(server, 3, 3, 3, 3)
		bit := uint64(0)
		if server {
			bit = 1
		}
		bidi, _ := s.Open(false)
		uni, _ := s.Open(true)
		if bidi != bit || uni != bit+2 {
			t.Fatal("wrong local stream types")
		}
		for _, id := range []uint64{bidi, bit ^ 1} {
			if err := s.Observe(id, true); err != nil {
				t.Fatal(err)
			}
			if err := s.Observe(id, false); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Observe(uni, true); !errors.Is(err, ErrStreamState) {
			t.Fatal("peer sent on local unidirectional", err)
		}
		if err := s.Observe(uni, false); err != nil {
			t.Fatal(err)
		}
		peerUni := (bit ^ 1) + 2
		if err := s.Observe(peerUni, false); !errors.Is(err, ErrStreamState) {
			t.Fatal("peer controlled receive-only stream", err)
		}
		if err := s.Observe(peerUni, true); err != nil {
			t.Fatal(err)
		}
		if err := s.Observe(bit+4, true); !errors.Is(err, ErrStreamState) {
			t.Fatal("unopened local stream", err)
		}
	}
}
func TestImplicitOpeningAndLimits(t *testing.T) {
	s, _ := NewIDs(false, 3, 0, 1, 0)
	if err := s.Observe(9, false); err != nil {
		t.Fatal(err)
	} // Peer stream 9 opens 1, 5, and 9.
	if s.Pending(false) != 3 {
		t.Fatal("implicit streams missing")
	}
	for _, want := range []uint64{1, 5, 9} {
		got, err := s.Accept(false)
		if err != nil || got != want {
			t.Fatal(got, err)
		}
	}
	if _, err := s.Accept(false); !errors.Is(err, ErrWouldBlock) {
		t.Fatal(err)
	}
	if err := s.Observe(13, true); !errors.Is(err, ErrStreamLimit) {
		t.Fatal(err)
	}
	if s.Pending(false) != 0 {
		t.Fatal("rejected frame opened stream")
	}
	s.Open(false)
	if _, err := s.Open(false); !errors.Is(err, ErrWouldBlock) {
		t.Fatal(err)
	}
	s.UpdateSendLimit(false, 2)
	s.UpdateSendLimit(false, 0)
	if id, err := s.Open(false); err != nil || id != 4 {
		t.Fatal(id, err)
	}
}
func TestMaxStreamIDDoesNotAllocatePrecedingStreams(t *testing.T) {
	s, _ := NewIDs(false, MaxStreams, MaxStreams, MaxStreams, MaxStreams)
	if err := s.Observe(wire.MaxVarint, true); err != nil {
		t.Fatal(err)
	}
	if s.Pending(true) != MaxStreams {
		t.Fatal(s.Pending(true))
	}
	if id, err := s.Accept(true); err != nil || id != 3 {
		t.Fatal(id, err)
	}
	if err := s.UpdateSendLimit(true, MaxStreams+1); !errors.Is(err, ErrStreamLimit) {
		t.Fatal(err)
	}
	if err := s.Observe(wire.MaxVarint+1, true); !errors.Is(err, ErrStreamState) {
		t.Fatal(err)
	}
}
