package qpack

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestSectionPrefixExamples(t *testing.T) {
	for _, tc := range []struct {
		wire            []byte
		capacity, total uint64
		want            SectionPrefix
	}{
		{[]byte{0, 0}, 0, 0, SectionPrefix{0, 0}},
		// RFC 9204 4.5.1: capacity 100, total 10, encoded 4 => required 9;
		// negative delta 2 => base 6.
		{[]byte{4, 130}, 100, 10, SectionPrefix{9, 6}},
		{[]byte{4, 2}, 100, 10, SectionPrefix{9, 11}},
		{[]byte{2, 0}, 32, 0, SectionPrefix{1, 1}}, // Future insert: blocked, not invalid.
		{[]byte{0, 7}, 0, 0, SectionPrefix{0, 7}},  // Arbitrary base without dynamic refs.
	} {
		p, n, err := ConsumeSectionPrefix(tc.wire, tc.capacity, tc.total)
		if err != nil || p != tc.want || n != len(tc.wire) {
			t.Fatalf("%v %d %v", p, n, err)
		}
		b, err := AppendSectionPrefix(nil, p, tc.capacity)
		if err != nil || !bytes.Equal(b, tc.wire) {
			t.Fatalf("%x %v", b, err)
		}
		for i := 0; i < len(tc.wire); i++ {
			_, n, err := ConsumeSectionPrefix(tc.wire[:i], tc.capacity, tc.total)
			if !errors.Is(err, io.ErrUnexpectedEOF) || n != 0 {
				t.Fatal(n, err)
			}
		}
	}
}
func TestInsertCountWrapWindows(t *testing.T) {
	// Independent oracle: within (total-entries,total+entries], a nonzero
	// absolute count has one modulo representation. Enumerate that window.
	for entries := uint64(1); entries < 17; entries++ {
		for total := uint64(0); total < 100; total++ {
			for encoded := uint64(1); encoded <= 2*entries+1; encoded++ {
				var want uint64
				for candidate := uint64(1); candidate <= total+entries; candidate++ {
					if candidate+entries <= total {
						continue
					}
					if candidate%(2*entries)+1 == encoded {
						want = candidate
					}
				}
				got, err := reconstructInsertCount(encoded, entries*32, total)
				if want == 0 {
					if err == nil {
						t.Fatalf("accepted %d/%d/%d => %d", entries, total, encoded, got)
					}
				} else if err != nil || got != want {
					t.Fatalf("%d/%d/%d => %d %v want %d", entries, total, encoded, got, err, want)
				}
			}
		}
	}
}
func TestSectionPrefixInvalidAndLimits(t *testing.T) {
	for _, tc := range []struct {
		b               []byte
		capacity, total uint64
	}{
		{[]byte{1, 0}, 0, 0}, {[]byte{1, 0}, 31, 0}, {[]byte{7, 0}, 100, 10},
		{[]byte{1, 0}, 32, 0}, {[]byte{0, 128}, 0, 0}, {[]byte{2, 129}, 32, 0},
	} {
		_, n, err := ConsumeSectionPrefix(tc.b, tc.capacity, tc.total)
		if err == nil || n != 0 {
			t.Fatalf("accepted %x", tc.b)
		}
	}
	for _, capacity := range []uint64{32, 100, MaxInteger} {
		want := SectionPrefix{MaxInteger, MaxInteger}
		b, err := AppendSectionPrefix(nil, want, capacity)
		if err != nil {
			t.Fatal(err)
		}
		got, _, err := ConsumeSectionPrefix(b, capacity, MaxInteger)
		if err != nil || got != want {
			t.Fatalf("boundary %v %v", got, err)
		}
	}
	b, _ := AppendSectionPrefix(nil, SectionPrefix{1, 1}, 32)
	b, _ = AppendInteger(b[:1], 0, 7, MaxInteger)
	if _, _, err := ConsumeSectionPrefix(b, 32, 0); err == nil {
		t.Fatal("base overflow accepted")
	}
	if _, _, err := ConsumeSectionPrefix([]byte{1, 0}, 32, MaxInteger); err != ErrIntegerOverflow {
		t.Fatalf("reconstructed count overflow: %v", err)
	}
	dst := []byte{99}
	for _, p := range []SectionPrefix{{1, 0}, {MaxInteger + 1, 0}, {0, MaxInteger + 1}} {
		got, err := AppendSectionPrefix(dst, p, 0)
		if err == nil || !bytes.Equal(got, dst) {
			t.Fatal("nontransactional error")
		}
	}
}
func FuzzSectionPrefix(f *testing.F) {
	f.Add([]byte{4, 130}, uint64(100), uint64(10))
	f.Add([]byte{0, 0}, uint64(0), uint64(0))
	f.Fuzz(func(t *testing.T, b []byte, capacity, total uint64) {
		p, n, err := ConsumeSectionPrefix(b, capacity, total)
		if err != nil {
			if n != 0 {
				t.Fatal("error consumed input")
			}
			return
		}
		encoded, err := AppendSectionPrefix(nil, p, capacity)
		if err != nil {
			t.Fatal(err)
		}
		again, m, err := ConsumeSectionPrefix(encoded, capacity, total)
		if err != nil || again != p || m != len(encoded) || n > len(b) {
			t.Fatal("prefix round trip")
		}
	})
}
