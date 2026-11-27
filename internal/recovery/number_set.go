package recovery

import (
	"slices"

	"github.com/getlantern/photon/internal/wire"
)

// numberSet is an ascending set of packet numbers that recovery no longer
// tracks but a later ACK may still cover: lost packets' history records, and
// ECN marks of packets lost, abandoned or evicted. An ACK range finds its
// members by binary search and steps over removed ones, so an ACK costs what
// it covers, not the set's size. An honest ACK's gaps are exactly where
// losses were, so lost numbers that stay lost are never visited.
type numberSet struct {
	nums []uint64
	// next[i] leads toward the first member at or after index i: itself
	// while i is a member, a later index once removed. Lookups compress the
	// paths they follow, so runs of removed entries, late ACKs of lost
	// packets that ranges keep covering, cost almost nothing to step over.
	// next[len(nums)] is a sentinel.
	next []int
	dead int
}

func (s *numberSet) Len() int { return len(s.nums) - s.dead }

// live returns the index of the first member at or after i, or len(nums).
func (s *numberSet) live(i int) int {
	root := i
	for s.next[root] != root {
		root = s.next[root]
	}
	for s.next[i] != root {
		i, s.next[i] = s.next[i], root
	}
	return root
}

// add inserts pn. Numbers usually arrive ascending and append; any other
// insertion rebuilds the set.
func (s *numberSet) add(pn uint64) {
	if n := len(s.nums); n == 0 || s.nums[n-1] < pn {
		if s.next == nil {
			s.next = []int{0}
		}
		s.nums = append(s.nums, pn)
		s.next = append(s.next, n+1)
		return
	}
	i, found := slices.BinarySearch(s.nums, pn)
	if found && s.live(i) == i {
		return
	}
	s.compact(true)
	if i, found = slices.BinarySearch(s.nums, pn); !found {
		s.nums = slices.Insert(s.nums, i, pn)
		s.next = append(s.next, len(s.nums))
	}
}

func (s *numberSet) remove(pn uint64) {
	if i, found := slices.BinarySearch(s.nums, pn); found && s.live(i) == i {
		s.next[i] = i + 1
		s.dead++
		s.compact(false)
	}
}

// covered calls visit, in ascending order, for each member f acknowledges,
// and removes the member when visit returns true.
func (s *numberSet) covered(f wire.AckFrame, visit func(uint64) bool) {
	if len(s.nums) == 0 {
		return
	}
	for r := len(f.Ranges) - 1; r >= 0; r-- {
		span := f.Ranges[r]
		i, _ := slices.BinarySearch(s.nums, span.Smallest)
		for i = s.live(i); i < len(s.nums) && s.nums[i] <= span.Largest; i = s.live(i + 1) {
			if visit(s.nums[i]) {
				s.next[i] = i + 1
				s.dead++
			}
		}
	}
	s.compact(false)
}

// compact drops removed entries, when forced or once they are half the set,
// so the set's memory and binary searches follow its members.
func (s *numberSet) compact(force bool) {
	if s.dead == 0 || !force && 2*s.dead < len(s.nums) {
		return
	}
	j := 0
	for i := s.live(0); i < len(s.nums); i = s.live(i + 1) {
		s.nums[j] = s.nums[i]
		j++
	}
	s.nums = s.nums[:j]
	s.next = s.next[:j+1]
	for i := range s.next {
		s.next[i] = i
	}
	s.dead = 0
}

func (s *numberSet) clear() { s.nums, s.next, s.dead = nil, nil, 0 }
