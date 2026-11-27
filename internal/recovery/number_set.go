package recovery

import (
	"slices"

	"github.com/getlantern/photon/internal/wire"
)

// numberSet is an ascending set of packet numbers that recovery no longer
// tracks but a later ACK may still cover: lost packets' history records, and
// ECN marks of packets lost, abandoned or evicted. An ACK range finds its
// members by binary search, so an ACK costs what it covers, not the set's
// size. An honest ACK's gaps are exactly where losses were, so lost numbers
// that stay lost are never visited.
type numberSet struct {
	nums []uint64
	gone []bool // removed, awaiting compaction
	dead int
}

func (s *numberSet) Len() int { return len(s.nums) - s.dead }

// add inserts pn. Numbers usually arrive ascending and append.
func (s *numberSet) add(pn uint64) {
	if n := len(s.nums); n == 0 || s.nums[n-1] < pn {
		s.nums = append(s.nums, pn)
		s.gone = append(s.gone, false)
		return
	}
	i, found := slices.BinarySearch(s.nums, pn)
	if found {
		if s.gone[i] {
			s.gone[i] = false
			s.dead--
		}
		return
	}
	s.nums = slices.Insert(s.nums, i, pn)
	s.gone = slices.Insert(s.gone, i, false)
}

func (s *numberSet) remove(pn uint64) {
	if i, found := slices.BinarySearch(s.nums, pn); found && !s.gone[i] {
		s.gone[i] = true
		s.dead++
		s.compact()
	}
}

// covered calls visit, in ascending order, for each member f acknowledges,
// and removes the member when visit returns true.
func (s *numberSet) covered(f wire.AckFrame, visit func(uint64) bool) {
	for r := len(f.Ranges) - 1; r >= 0; r-- {
		span := f.Ranges[r]
		i, _ := slices.BinarySearch(s.nums, span.Smallest)
		for ; i < len(s.nums) && s.nums[i] <= span.Largest; i++ {
			if !s.gone[i] && visit(s.nums[i]) {
				s.gone[i] = true
				s.dead++
			}
		}
	}
	s.compact()
}

// compact drops removed members once they are half the set, so lookups
// stay logarithmic in the members present.
func (s *numberSet) compact() {
	if s.dead == 0 || 2*s.dead < len(s.nums) {
		return
	}
	j := 0
	for i, pn := range s.nums {
		if !s.gone[i] {
			s.nums[j], s.gone[j] = pn, false
			j++
		}
	}
	s.nums, s.gone, s.dead = s.nums[:j], s.gone[:j], 0
}

func (s *numberSet) clear() { s.nums, s.gone, s.dead = nil, nil, 0 }
