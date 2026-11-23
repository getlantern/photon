package transport

import (
	"encoding/binary"
	"github.com/getlantern/photon/internal/wire"
	"testing"
	"time"
)

// The oracle stores individual packet membership, independently of the range
// insertion/merging implementation. Eviction retains the newest bounded runs.
type replayHistoryModel struct {
	seen     [512]bool
	floor    int
	capacity int
}

func (m *replayHistoryModel) contains(pn int) bool { return pn < m.floor || m.seen[pn] }
func (m *replayHistoryModel) record(pn int) bool {
	if m.contains(pn) {
		return false
	}
	m.seen[pn] = true
	runs := 0
	inside := false
	for n := len(m.seen) - 1; n >= m.floor; n-- {
		if m.seen[n] && !inside {
			runs++
		}
		inside = m.seen[n]
		if runs > m.capacity {
			boundary := n + 1
			for boundary < len(m.seen) && !m.seen[boundary] {
				boundary++
			}
			for old := m.floor; old < boundary; old++ {
				m.seen[old] = false
			}
			m.floor = boundary
			break
		}
	}
	return pn >= m.floor
}

func FuzzCandidateChallengeReplayHistory(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 1, 2, 0, 2})
	var eviction []byte
	for pn := 0; pn < 200; pn += 2 {
		eviction = append(eviction, 0, byte(pn>>8), byte(pn))
	}
	eviction = append(eviction, 0, 0, 0, 0, 0, 1, 0, 0, 197, 1, 0, 199, 0, 0, 199)
	f.Add(eviction)
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1536 {
			data = data[:1536]
		}
		received, _ := NewReceived(8)
		a := &pathApplication{space: &numberSpace{received: received}}
		normal := replayHistoryModel{capacity: 8}
		candidate := replayHistoryModel{capacity: 64}
		now := time.Unix(101, 0)
		for offset := 0; offset+2 < len(data); offset += 3 {
			pn := int(binary.BigEndian.Uint16(data[offset+1:offset+3]) % 512)
			switch data[offset] % 3 {
			case 0:
				expected := false
				if !normal.contains(pn) {
					expected = candidate.record(pn)
				}
				before := received.Expected()
				fresh, err := a.recordCandidateChallenges(uint64(pn), []wire.Frame{wire.PathChallengeFrame{}}, now)
				if err != nil || fresh != expected {
					t.Fatalf("candidate pn=%d fresh=%v want=%v err=%v", pn, fresh, expected, err)
				}
				if received.Expected() != before || a.space.ackPending {
					t.Fatal("candidate altered admission")
				}
			case 1:
				expected := normal.record(pn)
				receipt, err := received.Record(uint64(pn), now)
				if err != nil || (receipt == NewPacket) != expected {
					t.Fatal("ordinary receipt differs from membership oracle", pn, receipt, expected, err)
				}
			case 2:
				prior := a.candidateChallenges
				fresh, err := a.recordCandidateChallenges(uint64(pn), []wire.Frame{wire.PingFrame{}}, now)
				if err != nil || fresh || a.candidateChallenges != prior {
					t.Fatal("unrelated frame changed challenge history")
				}
			}
			if a.candidateChallenges != nil && (len(a.candidateChallenges.ranges) > 64 || int(a.candidateChallenges.floor) != candidate.floor) {
				t.Fatal("candidate history bound/floor")
			}
		}
		for pn := 0; pn < 512; pn++ {
			if receivedPacketNumber(received, uint64(pn)) != normal.contains(pn) || receivedPacketNumber(a.candidateChallenges, uint64(pn)) != candidate.contains(pn) {
				t.Fatal("membership mismatch", pn)
			}
		}
	})
}
