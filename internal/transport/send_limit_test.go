package transport

import (
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

// fixedWindow reports a set congestion window, as a controller whose window
// has grown, or not, would.
type fixedWindow struct {
	recovery.Controller
	window uint64
}

func (w fixedWindow) Window() uint64 { return w.window }

// Stream data is admitted up to streamSendWindows congestion windows, never
// below minStreamSendBytes or above maxSendBytes: a peer that never
// acknowledges anything keeps the window, and so the connection's buffer,
// small.
func TestStreamAdmissionFollowsWindow(t *testing.T) {
	c := ledgerPair(t)
	id, err := c.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		window uint64
		limit  int
	}{
		{12000, minStreamSendBytes},
		{1 << 20, streamSendWindows << 20},
		{8 << 20, maxSendBytes - controlReserve},
	} {
		c.congestion = fixedWindow{c.congestion, tc.window}
		if got := c.streamSendLimit(); got != tc.limit {
			t.Fatalf("window %d: limit %d, want %d", tc.window, got, tc.limit)
		}
		saved := c.queuedBytes
		c.queuedBytes = tc.limit - 100
		if n, err := c.WriteStream(id, make([]byte, 1000), false); n != 0 || !errors.Is(err, stream.ErrWouldBlock) {
			t.Fatalf("window %d: admitted %d past the stream limit: %v", tc.window, n, err)
		}
		c.queuedBytes = tc.limit - 2000
		if n, err := c.WriteStream(id, make([]byte, 1000), false); n != 1000 || err != nil {
			t.Fatalf("window %d: refused %d under the stream limit: %v", tc.window, n, err)
		}
		c.queuedBytes = saved + c.queuedBytes - (tc.limit - 2000)
	}
}

// A window that shrinks below what is queued, after a loss, holds back new
// stream data but not control frames, which still have maxSendBytes: the
// connection stays open.
func TestShrunkWindowHoldsOnlyStreamData(t *testing.T) {
	c := ledgerPair(t)
	id, err := c.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	c.congestion = fixedWindow{c.congestion, 12000}
	c.queuedBytes = minStreamSendBytes + 4096
	if n, err := c.WriteStream(id, make([]byte, 100), false); n != 0 || !errors.Is(err, stream.ErrWouldBlock) {
		t.Fatal("stream data admitted past a shrunk limit", n, err)
	}
	if err := c.enqueue(applicationSpace, wire.PingFrame{}, 1); err != nil || c.closed {
		t.Fatal("control frame refused past the stream limit", err)
	}
	if err := c.check(); err != nil {
		t.Fatal(err)
	}
}

// TryWriteStreamAll checks the whole slice against the stream limit, the
// one WriteStream applies: a slice that would fit maxSendBytes but not the
// stream limit waits whole, rather than being written in part.
func TestTryWriteAllUsesStreamLimit(t *testing.T) {
	c := ledgerPair(t)
	id, err := c.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	c.congestion = fixedWindow{c.congestion, 12000}
	// Room for the first chunk, not the slice: all or nothing.
	c.queuedBytes = minStreamSendBytes - 2*portableStreamPayload
	if err := c.TryWriteStreamAll(id, make([]byte, 3*portableStreamPayload)); !errors.Is(err, stream.ErrWouldBlock) {
		t.Fatal("slice past the stream limit not refused", err)
	}
	if off := c.streams.objects[id].send.Offset(); off != 0 {
		t.Fatal("part of the slice written", off)
	}
	if err := c.check(); err != nil {
		t.Fatal("connection failed", err)
	}
}

// With multipath, the limit follows the sum of the paths' windows.
func TestStreamLimitSumsPathWindows(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	var want uint64
	for id, state := range c.multipath.paths {
		if state.application == nil {
			continue
		}
		w := uint64(id+1) << 20
		state.application.congestion = fixedWindow{state.application.congestion, w}
		want += w
	}
	if got := c.streamSendLimit(); got != int(min(streamSendWindows*want, maxSendBytes-controlReserve)) {
		t.Fatal("multipath limit", got, "want", streamSendWindows*want)
	}
}

// Stream data stops controlReserve short of maxSendBytes, so with the
// stream limit full a control frame still fits: a full limit never fails
// the connection on a flow-control update or a path response.
func TestControlFramesFitBesideFullStreamLimit(t *testing.T) {
	c := ledgerPair(t)
	c.congestion = fixedWindow{c.congestion, 8 << 20}
	if c.streamSendLimit() != maxSendBytes-controlReserve {
		t.Fatal("stream limit", c.streamSendLimit())
	}
	c.queuedBytes = c.streamSendLimit()
	for range 1000 {
		if err := c.enqueue(applicationSpace, wire.MaxDataFrame{Maximum: 1 << 30}, 32); err != nil {
			t.Fatal("control frame refused beside a full stream limit", err)
		}
	}
	if err := c.check(); err != nil {
		t.Fatal(err)
	}
}

// A slice needing more frames than maxQueuedFrames, which the larger send
// buffer allows, goes whole into an empty queue rather than waiting forever.
func TestTryWriteAllLargeSliceIntoEmptyQueue(t *testing.T) {
	p := streamParameters()
	p.InitialMaxData = 64 << 20
	p.InitialMaxStreamDataBidiLocal, p.InitialMaxStreamDataBidiRemote = 16<<20, 16<<20
	client, server := testPair(t, wire.Version1, p)
	drivePair(t, client, server, false, false)
	client.congestion = fixedWindow{client.congestion, 4 << 20}
	id, err := client.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 2<<20)
	if err := client.TryWriteStreamAll(id, data); err != nil {
		t.Fatal("large slice refused by an empty queue", err)
	}
	if client.streams.objects[id].send.Offset() != uint64(len(data)) || client.queuedFrames() <= maxQueuedFrames {
		t.Fatal("slice not admitted whole", client.queuedFrames())
	}
}

// A flood of PATH_CHALLENGEs leaves at most maxPathResponses responses
// waiting, as quic-go has it, rather than one each.
func TestPathChallengeFloodBounded(t *testing.T) {
	client, server := testPair(t, wire.Version1, streamParameters())
	drivePair(t, client, server, false, false)
	now := time.Unix(200, 0)
	for pn := range uint64(3) {
		frames := make([]wire.Frame, 100)
		for i := range frames {
			frames[i] = wire.PathChallengeFrame{Data: [8]byte{byte(pn), byte(i)}}
		}
		if err := client.HandleDatagram(makePeerPacket(t, server, applicationSpace, 1000+pn, frames...), now); err != nil {
			t.Fatal(err)
		}
	}
	responses := 0
	for _, item := range client.spaces[applicationSpace].queue {
		if _, ok := item.frame.(wire.PathResponseFrame); ok {
			responses++
		}
	}
	if responses != maxPathResponses {
		t.Fatal("PATH_RESPONSEs waiting", responses)
	}
}

// A peer that retires every earlier connection ID with each new one queues a
// RETIRE_CONNECTION_ID per frame; past twice our active_connection_id_limit
// waiting, the connection fails with CONNECTION_ID_LIMIT_ERROR. A repeated
// frame retires nothing new.
func TestRetireFloodBounded(t *testing.T) {
	client, server := testPair(t, wire.Version1, streamParameters())
	drivePair(t, client, server, false, false)
	now := time.Unix(200, 0)
	limit := 2 * int(max(client.config.Parameters.ActiveConnectionIDLimit, 2))
	next := func(seq uint64) wire.Frame {
		return wire.NewConnectionIDFrame{Sequence: seq, RetirePriorTo: seq, ConnectionID: []byte{0xc0, byte(seq >> 8), byte(seq), 1, 2, 3, 4, 5}, StatelessResetToken: [16]byte{byte(seq)}}
	}
	pn := uint64(1000)
	send := func(f wire.Frame) error {
		pn++
		return client.HandleDatagram(makePeerPacket(t, server, applicationSpace, pn, f), now)
	}
	waiting := func() int {
		n := 0
		for _, item := range client.spaces[applicationSpace].queue {
			if _, ok := item.frame.(wire.RetireConnectionIDFrame); ok {
				n++
			}
		}
		return n
	}
	for seq := uint64(10); seq < 10+10*uint64(limit); seq++ {
		before := waiting()
		err := send(next(seq))
		if err == nil {
			if err := send(next(seq)); err != nil || waiting() > limit {
				t.Fatal("repeated frame failed or queued more", err, waiting())
			}
			continue
		}
		var te *TransportError
		if !errors.As(err, &te) || te.Code != 9 {
			t.Fatal("flood failed with", err)
		}
		if before != limit {
			t.Fatal("failed with", before, "retirements waiting, want", limit)
		}
		return
	}
	t.Fatal("flood never failed the connection")
}

// Real connections activate the base path at confirmation, whenever
// migration is on, and their challenges reach queuePathResponse: the same
// bound holds there.
func TestActivatedPathChallengeFloodBounded(t *testing.T) {
	client, server := testPair(t, wire.Version1, streamParameters())
	drivePair(t, client, server, false, false)
	local := netip.MustParseAddrPort("192.0.2.1:443")
	remote := netip.MustParseAddrPort("192.0.2.2:444")
	if err := client.activateBasePath(local, remote); err != nil {
		t.Fatal(err)
	}
	if err := server.activateBasePath(remote, local); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(200, 0)
	for pn := range uint64(5) {
		frames := make([]wire.Frame, 100)
		for i := range frames {
			frames[i] = wire.PathChallengeFrame{Data: [8]byte{byte(pn), byte(i)}}
		}
		if err := client.handleBoundMultipathPacket(makePeerPacket(t, server, applicationSpace, 1000+pn, frames...), local, remote, now); err != nil {
			t.Fatal(err)
		}
	}
	responses := 0
	for _, item := range client.multipath.paths[0].application.space.queue {
		if _, ok := item.frame.(wire.PathResponseFrame); ok {
			responses++
		}
	}
	if responses != maxPathResponses {
		t.Fatal("PATH_RESPONSEs waiting", responses)
	}
	if err := client.check(); err != nil {
		t.Fatal(err)
	}
}

// An abandoned path carries no new data, so its window no longer counts.
func TestStreamLimitSkipsAbandonedPaths(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	c.multipath.paths[0].application.congestion = fixedWindow{c.multipath.paths[0].application.congestion, 12000}
	c.multipath.paths[1].application.congestion = fixedWindow{c.multipath.paths[1].application.congestion, 4 << 20}
	if got := c.streamSendLimit(); got != maxSendBytes-controlReserve {
		t.Fatal("limit with both paths", got)
	}
	if err := c.abandonFailedPath(1); err != nil {
		t.Fatal(err)
	}
	if got := c.streamSendLimit(); got != minStreamSendBytes {
		t.Fatal("abandoned path's window still counted", got)
	}
}

// RETIREs count until acknowledged, not until sent: a flood whose RETIREs
// go out but are never acknowledged still fails at the bound.
func TestRetireFloodCountsSentUnacknowledged(t *testing.T) {
	client, server := testPair(t, wire.Version1, streamParameters())
	drivePair(t, client, server, false, false)
	client.congestion = openController{client.congestion}
	now := time.Unix(200, 0)
	pn := uint64(1000)
	for seq := uint64(10); seq < 200; seq++ {
		pn++
		f := wire.NewConnectionIDFrame{Sequence: seq, RetirePriorTo: seq, ConnectionID: []byte{0xd0, byte(seq >> 8), byte(seq), 1, 2, 3, 4, 5}, StatelessResetToken: [16]byte{byte(seq), 1}}
		err := client.HandleDatagram(makePeerPacket(t, server, applicationSpace, pn, f), now)
		if err != nil {
			var te *TransportError
			if !errors.As(err, &te) || te.Code != 9 {
				t.Fatal("flood failed with", err)
			}
			return
		}
		// Send everything queued; the peer never acknowledges it.
		for {
			out, err := client.NextDatagram(now)
			if err != nil {
				t.Fatal(err)
			}
			if out == nil {
				break
			}
		}
		now = now.Add(time.Millisecond)
	}
	t.Fatal("unacknowledged RETIREs never bounded the flood")
}

// The exact count finds a path's RETIREs wherever they wait or fly, and
// only that path's.
func TestCountRetirementsAcrossQueues(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	zero := c.multipath.paths[0].application.space
	zero.queue = append(zero.queue, &sendItem{frame: wire.PathRetireConnectionIDFrame{PathID: 1, Sequence: 7}, queued: true})
	zero.sent[1<<40] = &sendItem{frame: wire.PathRetireConnectionIDFrame{PathID: 1, Sequence: 8}}
	zero.queue = append(zero.queue, &sendItem{frame: wire.PathRetireConnectionIDFrame{PathID: 2, Sequence: 9}, queued: true})
	if n := c.countRetirements(1); n != 2 {
		t.Fatal("path 1 retirements", n)
	}
	if n := c.countRetirements(2); n != 1 {
		t.Fatal("path 2 retirements", n)
	}
}

// Retiring spent migration reservations shares the bound on unacknowledged
// RETIREs: with the bound reached, it fails the connection rather than
// queueing more.
func TestMigrationRetirementsBounded(t *testing.T) {
	client, _ := sendingPathPair(t, wire.Version1)
	peers := client.multipath.paths[1].peer
	candidate := &migrationCandidate{}
	seq, ok := peers.reserveMigrationID(candidate)
	if !ok || !peers.sentMigrationID(candidate, seq) || !peers.cancelMigrationID(candidate, seq) {
		t.Fatal("missing spent reservation")
	}
	limit := 2 * int(max(client.config.Parameters.ActiveConnectionIDLimit, 2))
	zero := client.multipath.paths[0].application.space
	for i := range limit {
		item := &sendItem{frame: wire.PathRetireConnectionIDFrame{PathID: 1, Sequence: uint64(1000 + i)}, queued: true}
		zero.queue = append(zero.queue, item)
		client.countRetire(item)
	}
	err := client.retireMigrationReservations(1, nil)
	var te *TransportError
	if !errors.As(err, &te) || te.Code != 9 || client.Err() == nil {
		t.Fatal("migration retirement past the bound", err)
	}
}
