package transport

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

func priorityFixture(ids ...uint64) (*Conn, *numberSpace) {
	c := &Conn{}
	c.streams.objects = make(map[uint64]*streamState)
	q := &numberSpace{}
	for _, id := range ids {
		c.streams.objects[id] = &streamState{send: &stream.Send{}, priority: streamPriority{3, true}}
		q.queue = append(q.queue, &sendItem{queued: true, frame: wire.StreamFrame{StreamID: id, Data: []byte{1}}})
	}
	return c, q
}

func priorityOrder(c *Conn, q *numberSpace) []uint64 {
	var ids []uint64
	for len(q.queue) > 0 {
		c.prioritizeStreams(q)
		item := q.queue[0]
		ids = append(ids, item.frame.(wire.StreamFrame).StreamID)
		c.commitStreamPriority(item)
		q.queue = q.queue[1:]
	}
	return ids
}

func TestStreamPriorityOrdering(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*Conn, *numberSpace)
		want  []uint64
	}{
		{"default round robin", nil, []uint64{0, 4, 8, 0, 4, 8}},
		{"urgency", func(c *Conn, _ *numberSpace) { c.SetStreamPriority(8, -9, true); c.SetStreamPriority(0, 127, true) }, []uint64{8, 8, 4, 4, 0, 0}},
		{"non incremental", func(c *Conn, _ *numberSpace) {
			for _, id := range []uint64{0, 4, 8} {
				c.SetStreamPriority(id, 3, false)
			}
		}, []uint64{0, 0, 4, 4, 8, 8}},
		{"mixed fairness", func(c *Conn, _ *numberSpace) { c.SetStreamPriority(0, 3, false); c.SetStreamPriority(4, 3, false) }, []uint64{0, 8, 0, 4, 8, 4}},
		{"retransmission", func(c *Conn, q *numberSpace) { c.SetStreamPriority(0, 0, true); q.queue[4].transmitted = true }, []uint64{4, 0, 0, 4, 8, 8}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c, q := priorityFixture(8, 8, 0, 0, 4, 4)
			if test.setup != nil {
				test.setup(c, q)
			}
			if got := priorityOrder(c, q); !reflect.DeepEqual(got, test.want) {
				t.Fatal(got, test.want)
			}
		})
	}
}

func TestStreamPrioritySelectionTransaction(t *testing.T) {
	c, q := priorityFixture(0, 0, 4, 4)
	c.prioritizeStreams(q)
	c.prioritizeStreams(q)
	if got := q.queue[0].frame.(wire.StreamFrame).StreamID; got != 0 {
		t.Fatal(got)
	}
	for _, cursor := range c.streams.priorityCursor {
		if cursor.valid {
			t.Fatal("selection consumed a turn")
		}
	}
	c.SetStreamPriority(4, 0, true)
	c.prioritizeStreams(q)
	if got := q.queue[0].frame.(wire.StreamFrame).StreamID; got != 4 {
		t.Fatal("queued update ignored", got)
	}
	control := &sendItem{queued: true, frame: wire.PingFrame{}}
	q.queue = append(q.queue, control)
	c.prioritizeStreams(q)
	if q.queue[len(q.queue)-1] != control {
		t.Fatal("control slot changed")
	}
}

func TestStreamPrioritySplitRetransmission(t *testing.T) {
	c, q := priorityFixture(0)
	q.queue[0].frame = wire.StreamFrame{StreamID: 0, Data: make([]byte, 2000)}
	q.queue[0].transmitted = true
	if err := c.splitQueuedStream(q, 0, 1000); err != nil {
		t.Fatal(err)
	}
	for _, item := range q.queue {
		if !item.transmitted {
			t.Fatal("split lost retransmission status")
		}
	}
}

func BenchmarkStreamPrioritySelection(b *testing.B) {
	for _, n := range []int{1, 16, 256, 4096} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			ids := make([]uint64, n)
			for i := range ids {
				ids[i] = uint64(i%16) * 4
			}
			c, q := priorityFixture(ids...)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c.prioritizeStreams(q)
				c.commitStreamPriority(q.queue[0])
			}
		})
	}
}
