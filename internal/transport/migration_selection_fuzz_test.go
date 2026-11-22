package transport

import (
	"net/netip"
	"testing"
	"time"
)

// This is a safety oracle, not an implementation of candidate selection. It
// records external nomination/proof events and checks every successful commit.
// Authentication and token matching are assumed at this selection-layer boundary.
func FuzzMigrationSelectionAuthorization(f *testing.F) {
	f.Add([]byte{0, 1, 10, 2, 0, 0, 1, 1, 11, 4, 0, 0})
	f.Add([]byte{0, 1, 10, 2, 0, 0, 0, 2, 11, 2, 0, 0, 1, 1, 9, 4, 0, 0, 1, 2, 12, 4, 0, 0})
	f.Add([]byte{0, 1, 10, 3, 0, 4, 0, 1, 10, 0, 2, 11, 1, 0, 20, 1, 2, 19, 2, 0, 0, 4, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 768 {
			data = data[:768]
		}
		local := netip.MustParseAddrPort("192.0.2.1:443")
		tuples := [3]pathTuple{}
		for i := range tuples {
			tuples[i] = pathTuple{local, netip.AddrPortFrom(netip.MustParseAddr("192.0.2.2"), uint16(444+i))}
		}
		m, err := newMigrationSelection(tuples[0])
		if err != nil {
			t.Fatal(err)
		}
		now := time.Unix(101, 0)
		births := map[*migrationCandidate]int{}
		proven := map[*migrationCandidate]bool{}
		var lastNumber uint64
		var lastTuple pathTuple
		lastNomination := -1
		for offset := 0; offset+2 < len(data); offset += 3 {
			op := data[offset] % 5
			tuple := tuples[int(data[offset+1])%len(tuples)]
			pn := uint64(data[offset+2])
			before := m.validated
			var err error
			switch op {
			case 0:
				if tuple != m.validated {
					_, err = m.observeProbingAuthenticated(tuple, pn, false, 1200, now, time.Second, false)
				}
			case 1:
				_, err = m.observeAuthenticated(tuple, pn, true, 1200, now, time.Second)
				if lastNomination < 0 || pn > lastNumber {
					lastNumber, lastTuple, lastNomination = pn, tuple, offset
				}
			case 2:
				if c := m.candidate; c != nil {
					c.validation.state = PathValidated
					proven[c] = true
				}
			case 3:
				now = now.Add(time.Duration(data[offset+2]%5) * time.Second)
				m.expire(now)
			case 4:
				c := m.candidate
				old, commitErr := m.commit(c)
				if commitErr == nil {
					born, known := births[c]
					if c == nil || !known || !proven[c] || lastNomination < born || lastTuple != c.tuple || old != before || m.validated != c.tuple || m.candidate != nil {
						t.Fatalf("unauthorized switch at %d: candidate=%+v nomination=%d birth=%d", offset, c, lastNomination, born)
					}
				} else if m.validated != before {
					t.Fatal("failed commit changed binding")
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if op != 4 && m.validated != before {
				t.Fatal("selection changed established binding")
			}
			if c := m.candidate; c != nil {
				if _, known := births[c]; !known {
					births[c] = offset
				}
				if m.canCommit(c) && (!proven[c] || lastTuple != c.tuple || lastNomination < births[c]) {
					t.Fatalf("candidate eligible without current proof and nomination at %d", offset)
				}
			}
		}
	})
}
