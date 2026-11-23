package transport

import (
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestMTUSearchConverges(t *testing.T) {
	for _, ceiling := range []int{1200, 1201, 1280, 1472, 9000, 65527} {
		for _, supported := range []int{1200, 1201, 1250, 1280, 1400, 1472, 9000, 65527} {
			supported = min(supported, ceiling)
			m, err := newMTUSearch(ceiling)
			if err != nil {
				t.Fatal(err)
			}
			number := uint64(0)
			for size, ok := m.next(); ok; size, ok = m.next() {
				if number > 60 {
					t.Fatal("search did not converge", ceiling, supported, m)
				}
				if err := m.sent(number, size); err != nil {
					t.Fatal(err)
				}
				if size <= supported {
					if !m.acknowledged(number) {
						t.Fatal("probe ACK ignored")
					}
				} else {
					if !m.lost(number) {
						t.Fatal("probe loss ignored")
					}
				}
				if m.confirmed > supported {
					t.Fatal("unverified size adopted", m.confirmed, supported)
				}
				number++
			}
			if m.confirmed != supported {
				t.Fatal("incorrect search result", ceiling, supported, m.confirmed)
			}
		}
	}
}

func TestMTUSearchReorderingAndReset(t *testing.T) {
	m, _ := newMTUSearch(1500)
	size, _ := m.next()
	before := m
	if again, ok := m.next(); !ok || again != size || m != before {
		t.Fatal("proposal mutated search")
	}
	if err := m.sent(10, size); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.next(); ok {
		t.Fatal("concurrent probe permitted")
	}
	if m.acknowledged(9) || m.lost(9) {
		t.Fatal("unrelated event accepted")
	}
	if !m.lost(10) || m.lost(10) {
		t.Fatal("duplicate loss accepted")
	}
	retry, _ := m.next()
	if retry != size || m.confirmed != 1200 {
		t.Fatal("single loss changed search")
	}
	if err := m.sent(11, retry); err != nil {
		t.Fatal(err)
	}
	if !m.acknowledged(10) || m.confirmed != size {
		t.Fatal("late ACK lost")
	}
	if m.acknowledged(11) || m.lost(11) {
		t.Fatal("superseded attempt changed search")
	}
	next, _ := m.next()
	if err := m.sent(12, next); err != nil {
		t.Fatal(err)
	}
	m.reset()
	if m.confirmed != 1200 || m.acknowledged(12) || m.lost(12) {
		t.Fatal("old tuple evidence survived reset")
	}
	resetSize, _ := m.next()
	before = m
	if err := m.sent(12, resetSize); err == nil || m != before {
		t.Fatal("reused packet number accepted")
	}
	if err := m.sent(13, resetSize); err != nil {
		t.Fatal(err)
	}
	if !m.acknowledged(13) {
		t.Fatal("new tuple proof ignored")
	}
}

func TestMTUSearchInvalidCommit(t *testing.T) {
	for _, ceiling := range []int{-1, 0, 1199, 65528} {
		if _, err := newMTUSearch(ceiling); err == nil {
			t.Fatal(ceiling)
		}
	}
	m, _ := newMTUSearch(1500)
	size, _ := m.next()
	before := m
	for _, tc := range []struct {
		pn   uint64
		size int
	}{{0, 1200}, {0, size + 1}, {wire.MaxVarint + 1, size}} {
		if err := m.sent(tc.pn, tc.size); err == nil || m != before {
			t.Fatal("invalid commit changed state", tc, m)
		}
	}
	if err := m.sent(wire.MaxVarint, size); err != nil {
		t.Fatal(err)
	}
	m.lost(wire.MaxVarint)
	if err := m.sent(wire.MaxVarint, size); err == nil {
		t.Fatal("packet number exhaustion ignored")
	}
}

func FuzzMTUSearch(f *testing.F) {
	f.Add(uint16(1500), []byte{0, 1, 0, 2, 3, 0, 4})
	f.Fuzz(func(t *testing.T, ceiling uint16, events []byte) {
		limit := max(1200, min(int(ceiling), 65527))
		m, _ := newMTUSearch(limit)
		var number uint64
		for _, event := range events {
			switch event % 5 {
			case 0:
				if size, ok := m.next(); ok {
					if err := m.sent(number, size); err != nil {
						t.Fatal(err)
					}
					number++
				}
			case 1:
				if number > 0 {
					m.acknowledged(number - 1)
				}
			case 2:
				if number > 0 {
					m.lost(number - 1)
				}
			case 3:
				m.reset()
			case 4:
				if m.acknowledged(number) || m.lost(number) {
					t.Fatal("unsent packet accepted")
				}
			}
			if m.confirmed < 1200 || m.confirmed > m.upper || m.upper > limit || m.attempts < 0 || m.attempts > 3 {
				t.Fatal("bounds", m)
			}
		}
	})
}
