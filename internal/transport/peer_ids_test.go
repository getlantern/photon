package transport

import (
	"errors"
	"github.com/getlantern/photon/internal/wire"
	"reflect"
	"testing"
)

func TestPeerIDRotation(t *testing.T) {
	p := newPeerIDs([]byte("initial"), 2)
	f := wire.NewConnectionIDFrame{Sequence: 1, ConnectionID: []byte("next"), StatelessResetToken: [16]byte{1}}
	if retired, err := p.add(f); err != nil || len(retired) != 0 {
		t.Fatal(retired, err)
	}
	if _, err := p.add(f); err != nil {
		t.Fatal("duplicate", err)
	}
	overflow := wire.NewConnectionIDFrame{Sequence: 2, ConnectionID: []byte("third")}
	if _, err := p.add(overflow); err == nil {
		t.Fatal("active limit")
	}
	if len(p.active) != 2 || p.current != 0 {
		t.Fatal("failed update changed state")
	}
	overflow.RetirePriorTo = 2
	retired, err := p.add(overflow)
	if err != nil || !reflect.DeepEqual(retired, []uint64{0, 1}) || p.current != 2 || len(p.active) != 1 {
		t.Fatal(retired, err)
	}
	// Sequence 1 is already retired: its repeat retires nothing again
	// (RFC 9000 Section 19.15) and doesn't restore it.
	if retired, err = p.add(f); err != nil || len(retired) != 0 || len(p.active) != 1 {
		t.Fatal("retired duplicate restored or retired again", retired, err)
	}
	overflow.ConnectionID[0] = 'X'
	if string(p.active[2].cid) != "third" {
		t.Fatal("CID aliases frame")
	}
}
func TestPeerIDConsistency(t *testing.T) {
	for _, f := range []wire.NewConnectionIDFrame{
		{Sequence: 0, ConnectionID: []byte("different")},
		{Sequence: 1, ConnectionID: []byte("initial")},
	} {
		p := newPeerIDs([]byte("initial"), 2)
		_, err := p.add(f)
		var te *TransportError
		if !errors.As(err, &te) || te.Code != 0xa {
			t.Fatal(err)
		}
	}
	p := newPeerIDs([]byte("initial"), 2)
	f := wire.NewConnectionIDFrame{Sequence: 1, ConnectionID: []byte("next"), StatelessResetToken: [16]byte{1}}
	p.add(f)
	f.StatelessResetToken[0] = 2
	if _, err := p.add(f); err == nil {
		t.Fatal("changed reset token")
	}
	p = newPeerIDs(nil, 2)
	if _, err := p.add(f); err == nil {
		t.Fatal("zero-length CID")
	}
}

func FuzzPeerIDs(f *testing.F) {
	f.Add([]byte{1, 0, 2, 1, 3, 2})
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 4096 {
			return
		}
		p := newPeerIDs([]byte{0xff}, 2)
		for i := 0; i+1 < len(input); i += 2 {
			seq := uint64(input[i])
			prior := uint64(input[i+1]) % (seq + 1)
			floor := p.retireBefore
			_, _ = p.add(wire.NewConnectionIDFrame{Sequence: seq, RetirePriorTo: prior, ConnectionID: []byte{byte(seq)}, StatelessResetToken: [16]byte{byte(seq)}})
			if len(p.active) < 1 || len(p.active) > 2 {
				t.Fatal("active CID count")
			}
			if _, ok := p.active[p.current]; !ok {
				t.Fatal("current CID missing")
			}
			if p.retireBefore < floor {
				t.Fatal("retirement floor regressed")
			}
			for seq := range p.active {
				if seq < p.retireBefore {
					t.Fatal("retired CID reactivated")
				}
			}
		}
	})
}
