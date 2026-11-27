package transport

import (
	"errors"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

func queueCapPair(t *testing.T) *Conn {
	t.Helper()
	p := streamParameters()
	p.InitialMaxData = 4 << 20
	p.InitialMaxStreamDataBidiLocal, p.InitialMaxStreamDataBidiRemote = 1<<20, 1<<20
	client, server := testPair(t, wire.Version1, p)
	drivePair(t, client, server, false, false)
	return client
}

// Each write is one frame, so tiny writes would otherwise build a queue of
// tens of thousands that every packet scans: data waits at maxQueuedFrames,
// though a FIN alone still goes.
func TestQueuedFramesBoundWrites(t *testing.T) {
	c := queueCapPair(t)
	id, err := c.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	for c.queuedFrames() < maxQueuedFrames {
		if n, err := c.WriteStream(id, []byte{1}, false); n != 1 || err != nil {
			t.Fatal("write", c.queuedFrames(), n, err)
		}
	}
	if n, err := c.WriteStream(id, []byte{1}, false); n != 0 || !errors.Is(err, stream.ErrWouldBlock) {
		t.Fatal("write past the frame bound", n, err)
	}
	if n, err := c.WriteStream(id, nil, true); n != 0 || err != nil {
		t.Fatal("FIN alone refused", err)
	}
	if err := c.check(); err != nil {
		t.Fatal(err)
	}
}

// TryWriteStreamAll stays all or nothing: a slice whose frames would pass
// the bound waits whole.
func TestTryWriteAllFrameBound(t *testing.T) {
	c := queueCapPair(t)
	id, err := c.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	for c.queuedFrames() < maxQueuedFrames-10 {
		if n, err := c.WriteStream(id, []byte{1}, false); n != 1 || err != nil {
			t.Fatal(err)
		}
	}
	offset := c.streams.objects[id].send.Offset()
	if err := c.TryWriteStreamAll(id, make([]byte, 20*portableStreamPayload)); !errors.Is(err, stream.ErrWouldBlock) {
		t.Fatal("slice past the frame bound admitted", err)
	}
	if c.streams.objects[id].send.Offset() != offset {
		t.Fatal("part of the slice written")
	}
	if err := c.TryWriteStreamAll(id, make([]byte, 10*portableStreamPayload)); err != nil || c.queuedFrames() != maxQueuedFrames {
		t.Fatal("slice within the bound refused", err, c.queuedFrames())
	}
}

// The cost of a packet with a full queue of 1-byte writes: each is a frame,
// and choosing each packet's stream scans the queued frames. Each measured
// send starts from a full queue, refilled outside the timer; a fresh pair
// replaces one whose sent-packet ledger is near its limit.
func BenchmarkTinyWritesNextDatagram(b *testing.B) {
	p := streamParameters()
	p.InitialMaxData = 64 << 20
	p.InitialMaxStreamDataBidiLocal, p.InitialMaxStreamDataBidiRemote = 16<<20, 16<<20
	var client *Conn
	var id uint64
	var sent, queued int
	fresh := func() {
		var server *Conn
		client, server = testPair(b, wire.Version1, p)
		drivePair(b, client, server, false, false)
		client.congestion = openController{client.congestion}
		var err error
		if id, err = client.OpenStream(false); err != nil {
			b.Fatal(err)
		}
		sent = 0
	}
	fill := func() {
		for {
			if w, err := client.WriteStream(id, []byte{1}, false); w != 1 || err != nil {
				break
			}
		}
		queued = client.queuedFrames()
	}
	now := time.Unix(200, 0)
	fresh()
	fill()
	b.ResetTimer()
	for range b.N {
		if out, err := client.NextDatagram(now); err != nil || out == nil {
			b.Fatal("no packet", err)
		}
		b.StopTimer()
		now = now.Add(time.Millisecond) // refills the pacer's burst
		if sent++; sent >= maxSentPackets/2 {
			fresh()
		}
		fill()
		b.StartTimer()
	}
	b.ReportMetric(float64(queued), "queued")
}

// With multipath the bound counts every path's frames together: frames
// split across paths block new stream data at the same total.
func TestQueuedFramesBoundAcrossPaths(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	id, err := c.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	for c.queuedFrames() < maxQueuedFrames {
		path := uint64(c.queuedFrames() % 2)
		if err := c.queueFrameOnPath(path, wire.PingFrame{}); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []uint64{0, 1} {
		if len(c.multipath.paths[path].application.space.queue) == 0 {
			t.Fatal("no frames on path", path)
		}
	}
	if n, err := c.WriteStream(id, []byte{1}, false); n != 0 || !errors.Is(err, stream.ErrWouldBlock) || c.streams.objects[id].send.Offset() != 0 {
		t.Fatal("write past the bound summed over paths", n, err)
	}
	one := c.multipath.paths[1].application.space
	one.queue = one.queue[1:]
	if n, err := c.WriteStream(id, []byte{1}, false); n != 1 || err != nil {
		t.Fatal("write under the bound refused", n, err)
	}
}
