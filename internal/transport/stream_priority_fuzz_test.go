package transport

import (
	"slices"
	"testing"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

// The oracle groups candidates by transmission class and stream, then sorts
// IDs. It deliberately doesn't mirror the production single-pass selection.
func referencePriorityIndex(c *Conn, q *numberSpace) int {
	groups := make([]map[uint64]int, 16)
	for i, item := range q.queue {
		f, ok := item.frame.(wire.StreamFrame)
		if !ok || item.acked {
			continue
		}
		priority := c.streams.objects[f.StreamID].priority
		class := int(priority.urgency)
		if !item.transmitted {
			class += 8
		}
		if groups[class] == nil {
			groups[class] = make(map[uint64]int)
		}
		if _, exists := groups[class][f.StreamID]; !exists {
			groups[class][f.StreamID] = i
		}
	}
	for class, group := range groups {
		if len(group) == 0 {
			continue
		}
		ids := make([]uint64, 0, len(group))
		for id := range group {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		candidates := make([]uint64, 0, len(ids))
		haveNonIncremental := false
		for _, id := range ids {
			if c.streams.objects[id].priority.incremental {
				candidates = append(candidates, id)
			} else if !haveNonIncremental {
				candidates = append(candidates, id)
				haveNonIncremental = true
			}
		}
		cursor := c.streams.priorityCursor[class]
		if cursor.valid {
			for _, id := range candidates {
				if id > cursor.id {
					return group[id]
				}
			}
		}
		return group[candidates[0]]
	}
	return -1
}

func FuzzStreamPrioritySelection(f *testing.F) {
	f.Add([]byte{0, 4, 8, 0, 4, 8})
	f.Add([]byte{31, 128, 64, 15, 255, 32, 8, 0})
	f.Add([]byte{255, 255, 255})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 128 {
			data = data[:128]
		}
		c := &Conn{}
		c.streams.objects = make(map[uint64]*streamState)
		q := &numberSpace{}
		for i, b := range data {
			id := uint64(b&15) * 4
			c.streams.objects[id] = &streamState{send: &stream.Send{}, priority: streamPriority{uint8(i % 8), b&16 != 0}}
			item := &sendItem{queued: true, acked: b&64 != 0, transmitted: b&32 != 0, frame: wire.StreamFrame{StreamID: id, Offset: uint64(i), Data: []byte{b}}}
			if b&128 != 0 {
				item.frame = wire.PingFrame{}
			}
			q.queue = append(q.queue, item)
		}
		for i := range c.streams.priorityCursor {
			c.streams.priorityCursor[i] = streamPriorityCursor{uint64(i%16) * 4, i%2 == 0}
		}
		for round := 0; len(q.queue) > 0; round++ {
			// Reprioritization while frames are queued exercises stale-index hazards.
			if len(data) > 0 {
				id := uint64(data[round%len(data)]&15) * 4
				c.SetStreamPriority(id, int8(data[round%len(data)]), round%2 == 0)
			}
			expected := referencePriorityIndex(c, q)
			if got := c.prioritizedStreamIndex(q); got != expected {
				t.Fatalf("selected %d, want %d", got, expected)
			}
			before := append([]*sendItem(nil), q.queue...)
			cursors := c.streams.priorityCursor
			c.prioritizeStreams(q)
			if c.streams.priorityCursor != cursors {
				t.Fatal("uncommitted selection moved cursor")
			}
			counts := make(map[*sendItem]int)
			for i, item := range before {
				counts[item]++
				if _, ok := item.frame.(wire.StreamFrame); !ok && q.queue[i] != item {
					t.Fatal("non-stream position changed")
				}
			}
			for _, item := range q.queue {
				counts[item]--
			}
			for _, count := range counts {
				if count != 0 {
					t.Fatal("selection duplicated or lost an item")
				}
			}
			if _, ok := before[0].frame.(wire.StreamFrame); ok && expected >= 0 && q.queue[0] != before[expected] {
				t.Fatal("selected item not at head")
			}
			if !q.queue[0].acked {
				c.commitStreamPriority(q.queue[0])
			}
			q.queue = q.queue[1:]
		}
	})
}
