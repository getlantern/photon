package wire

import (
	"bytes"
	"testing"
)

func TestVersionNegotiationCodec(t *testing.T) {
	for _, n := range []int{0, 8, 20, 255} {
		dcid, scid := bytes.Repeat([]byte{1}, n), bytes.Repeat([]byte{2}, 255-n)
		b, err := AppendVersionNegotiation(nil, 0, dcid, scid, []Version{Version1, Version2})
		if err != nil {
			t.Fatal(err)
		}
		h, err := ParseHeader(b, 0)
		if err != nil || h.Version != 0 || !h.Long || !bytes.Equal(h.DCID, dcid) || !bytes.Equal(h.SCID, scid) || len(h.Versions) != 2 || h.Versions[1] != Version2 {
			t.Fatal(h, err)
		}
		_, offset, err := ParseInvariantLongHeader(b)
		if err != nil {
			t.Fatal(err)
		}
		for end := 0; end < offset; end++ {
			if _, _, err := ParseInvariantLongHeader(b[:end]); err == nil {
				t.Fatal("accepted truncated invariant", end)
			}
		}
		if _, err := ParseHeader(b[:offset], 0); err == nil {
			t.Fatal("empty version list")
		}
	}
	for _, vs := range [][]Version{nil, {0}} {
		if _, err := AppendVersionNegotiation(nil, 0, nil, nil, vs); err == nil {
			t.Fatal("bad list")
		}
	}
	if _, err := AppendVersionNegotiation(nil, 0, make([]byte, 256), nil, []Version{Version1}); err == nil {
		t.Fatal("oversized CID")
	}
}
