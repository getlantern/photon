package transport

import (
	"errors"
	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
	"testing"
)

func TestWriteLimiterAdmission(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		p := streamParameters()
		p.InitialMaxData = 5
		p.InitialMaxStreamDataUni = 3
		c, s := testPair(t, v, p)
		drivePair(t, c, s, false, false)
		id, err := c.OpenStream(true)
		if err != nil {
			t.Fatal(err)
		}
		calls := 0
		limiter := func(maximum int) int {
			calls++
			if maximum != 3 {
				t.Fatalf("flow-controlled offer %d", maximum)
			}
			return 2
		}
		before := c.queuedBytes
		c.queuedBytes = maxSendBytes
		if n, err := c.WriteStreamWithLimit(id, []byte("abcdef"), limiter); n != 0 || !errors.Is(err, stream.ErrWouldBlock) || calls != 0 {
			t.Fatal("queue pressure charged limiter", n, err, calls)
		}
		c.queuedBytes = before
		if n, err := c.WriteStreamWithLimit(id, []byte("abcdef"), limiter); n != 2 || !errors.Is(err, ErrWriteLimitReached) {
			t.Fatal(n, err)
		}
		if c.streams.send.Used() != 2 || c.streams.objects[id].send.Offset() != 2 || c.queuedBytes != before+34 {
			t.Fatal("prefix accounting")
		}
		drivePair(t, c, s, true, true)
		if calls != 1 {
			t.Fatal("limiter charged on recovery", calls)
		}
		prefix := make([]byte, 2)
		if n, err := s.PeekStream(id, prefix); n != 2 || err != nil || string(prefix) != "ab" || c.queuedBytes != 0 {
			t.Fatal("recovered prefix", n, err, string(prefix))
		}
		for _, allowance := range []int{0, -10} {
			if n, err := c.WriteStreamWithLimit(id, []byte("cdef"), func(maximum int) int {
				if maximum != 1 {
					t.Fatal(maximum)
				}
				return allowance
			}); n != 0 || !errors.Is(err, ErrWriteLimitReached) {
				t.Fatal(n, err)
			}
			if c.streams.send.Used() != 2 {
				t.Fatal("zero allowance consumed credit")
			}
		}
		if n, err := c.WriteStreamWithLimit(id, []byte("cdef"), func(maximum int) int { return maximum + 100 }); n != 1 || err != nil {
			t.Fatal("clamp", n, err)
		}
		if n, err := c.WriteStreamWithLimit(id, []byte("def"), func(int) int { t.Fatal("limiter called without QUIC credit"); return 0 }); n != 0 || !errors.Is(err, stream.ErrWouldBlock) {
			t.Fatal(n, err)
		}
	}
}
