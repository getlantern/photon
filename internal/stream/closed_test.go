package stream

import (
	"github.com/getlantern/photon/internal/wire"
	"testing"
)

func TestClosedRangesMerge(t *testing.T) {
	var c Closed
	for _, id := range []uint64{8, 0, 16, 4, 12, 20, 3, wire.MaxVarint} {
		if err := c.Add(id); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []uint64{0, 4, 8, 12, 16, 20, 3, wire.MaxVarint} {
		if !c.Contains(id) {
			t.Fatal("missing", id)
		}
	}
	for _, id := range []uint64{1, 2, 7, 24, wire.MaxVarint - 4} {
		if c.Contains(id) {
			t.Fatal("false closed", id)
		}
	}
	if len(c.ranges[0]) != 1 {
		t.Fatal("adjacent ranges not merged", c.ranges[0])
	}
}
func TestClosedHistoryBoundedAcrossManyStreams(t *testing.T) {
	var c Closed
	for id := uint64(4); id < 400000; id += 4 {
		if err := c.Add(id); err != nil {
			t.Fatal(err)
		}
	}
	if c.Contains(0) || len(c.ranges[0]) != 1 {
		t.Fatal("active hole lost or history unbounded")
	}
	c.Add(0)
	if len(c.ranges[0]) != 1 || !c.Contains(0) {
		t.Fatal("prefix did not merge")
	}
}
