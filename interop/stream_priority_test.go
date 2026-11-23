package interop

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
)

// heldPacketConn withholds received packets from the peer while held, so our
// endpoint gets no acknowledgments and keeps everything past its initial
// congestion window queued.
type heldPacketConn struct {
	net.PacketConn
	mu   sync.Mutex
	held chan struct{}
}

// Upstream logs a warning when it cannot size the socket buffers.
func (c *heldPacketConn) SetReadBuffer(n int) error {
	return c.PacketConn.(*net.UDPConn).SetReadBuffer(n)
}
func (c *heldPacketConn) SetWriteBuffer(n int) error {
	return c.PacketConn.(*net.UDPConn).SetWriteBuffer(n)
}

func (c *heldPacketConn) hold() {
	c.mu.Lock()
	c.held = make(chan struct{})
	c.mu.Unlock()
}

func (c *heldPacketConn) release() {
	c.mu.Lock()
	if c.held != nil {
		close(c.held)
		c.held = nil
	}
	c.mu.Unlock()
}

func (c *heldPacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	n, addr, err := c.PacketConn.ReadFrom(p)
	c.mu.Lock()
	held := c.held
	c.mu.Unlock()
	if err == nil && held != nil {
		<-held
	}
	return n, addr, err
}

type streamArrival struct{ id, end uint64 }

type streamArrivalTrace struct {
	mu     sync.Mutex
	frames []streamArrival
}

func (t *streamArrivalTrace) AddProducer() qlogwriter.Recorder { return t }
func (t *streamArrivalTrace) SupportsSchemas(string) bool      { return true }
func (t *streamArrivalTrace) Close() error                     { return nil }
func (t *streamArrivalTrace) RecordEvent(event qlogwriter.Event) {
	p, ok := event.(qlog.PacketReceived)
	if !ok {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, frame := range p.Frames {
		if f, ok := frame.Frame.(*qlog.StreamFrame); ok {
			t.frames = append(t.frames, streamArrival{uint64(f.StreamID), uint64(f.Offset + f.Length)})
		}
	}
}

type streamPrioritySpec struct {
	urgency     int8
	incremental bool
}

// RFC 9002 initial congestion window: what the first-written stream can send,
// whatever its urgency, before the streams that precede it are queued.
const priorityInitialFlight = 14720

// arrivalOrderViolations replays what the peer received against the priorities
// of streams ids. A stream is preceded by every stream of lower urgency value
// and, when both are non-incremental at one urgency, by the lower ID. Counting
// only what arrives after a preceding stream starts would accept a fully
// reversed order, so everything before its completion is bounded too.
func arrivalOrderViolations(frames []streamArrival, ids []uint64, priorities []streamPrioritySpec, size, slack uint64) (violations []string) {
	index := map[uint64]int{}
	for i, id := range ids {
		index[id] = i
	}
	precedes := func(a, b int) bool {
		if priorities[a].urgency != priorities[b].urgency {
			return priorities[a].urgency < priorities[b].urgency
		}
		return !priorities[a].incremental && !priorities[b].incremental && ids[a] < ids[b]
	}
	shares := func(a, b int) bool {
		return a != b && priorities[a] == priorities[b] && priorities[a].incremental
	}
	high := make([]uint64, len(ids))
	started := make([]bool, len(ids))
	before := make([]uint64, len(ids))   // bytes of streams i precedes that arrived before i completed
	during := make([][]uint64, len(ids)) // of those, per stream, the bytes that arrived after i started
	lead := make([][]uint64, len(ids))   // the furthest i got ahead of a stream it shares turns with
	for i := range ids {
		during[i], lead[i] = make([]uint64, len(ids)), make([]uint64, len(ids))
	}
	for _, f := range frames {
		i, ok := index[f.id]
		if !ok || f.end <= high[i] {
			continue
		}
		fresh := f.end - high[i]
		high[i], started[i] = f.end, true
		for j := range ids {
			if precedes(j, i) && high[j] < size {
				before[j] += fresh
				if started[j] {
					during[j][i] += fresh
				}
			}
			if shares(i, j) && high[i] > high[j] {
				lead[i][j] = max(lead[i][j], high[i]-high[j])
			}
		}
	}
	for i := range ids {
		// An empty or partial trace would satisfy every bound below.
		if high[i] != size {
			violations = append(violations, fmt.Sprintf("trace has %d of %d bytes for stream %d (%+v)", high[i], size, i, priorities[i]))
		}
		if before[i] > priorityInitialFlight+slack {
			violations = append(violations, fmt.Sprintf("stream %d (%+v) completed after %d bytes of lower-priority streams", i, priorities[i], before[i]))
		}
		for j := range ids {
			if during[i][j] > slack {
				violations = append(violations, fmt.Sprintf("stream %d (%+v) yielded %d bytes to stream %d (%+v)", i, priorities[i], during[i][j], j, priorities[j]))
			}
			if lead[i][j] > priorityInitialFlight+slack {
				violations = append(violations, fmt.Sprintf("incremental stream %d led stream %d by %d bytes instead of sharing turns", i, j, lead[i][j]))
			}
		}
	}
	return violations
}

func TestStreamPriorityIndependent(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			ct, st := tlsConfigs(t)
			socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer socket.Close()
			gate := &heldPacketConn{PacketConn: socket}
			defer gate.release()
			trace := &streamArrivalTrace{}
			listener, err := upstream.Listen(gate, st, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}, Tracer: func(context.Context, bool, upstream.ConnectionID) qlogwriter.Trace { return trace }})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := quic.DialAddr(ctx, listener.Addr().String(), ct, &quic.Config{Versions: []quic.Version{version}})
			if err != nil {
				t.Fatal(err)
			}
			defer client.CloseWithError(0, "")
			server, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			// In ascending stream ID order: an incremental pair at the lowest
			// urgency, a non-incremental pair above it, and one urgent stream.
			// Lower priorities are queued first, so the default order would
			// serve them first.
			priorities := []streamPrioritySpec{{7, true}, {7, true}, {4, false}, {4, false}, {1, true}}
			const size = 64 << 10
			streams := make([]*quic.SendStream, len(priorities))
			payloads := map[uint64][]byte{}
			gate.hold()
			for i, priority := range priorities {
				streams[i], err = client.OpenUniStreamSync(ctx)
				if err != nil {
					t.Fatal(err)
				}
				streams[i].SetPriority(priority.urgency, priority.incremental)
				payload := bytes.Repeat([]byte{byte('a' + i)}, size)
				payloads[uint64(streams[i].StreamID())] = payload
				if err := streams[i].TryWriteAll(payload); err != nil {
					t.Fatal(i, err)
				}
			}
			gate.release()
			for i, s := range streams {
				s.SetWriteDeadline(time.Now().Add(3 * time.Second))
				if err := s.Close(); err != nil {
					t.Fatal(i, err)
				}
			}
			for range streams {
				received, err := server.AcceptUniStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				received.SetReadDeadline(time.Now().Add(3 * time.Second))
				got, err := io.ReadAll(received)
				if err != nil || !bytes.Equal(got, payloads[uint64(received.StreamID())]) {
					t.Fatal(received.StreamID(), len(got), err)
				}
			}
			trace.mu.Lock()
			frames := append([]streamArrival(nil), trace.frames...)
			trace.mu.Unlock()
			ids := make([]uint64, len(streams))
			for i, s := range streams {
				ids[i] = uint64(s.StreamID())
			}
			// Retransmissions legitimately precede fresh data of any priority,
			// so allow a few packets rather than requiring exactly zero.
			for _, violation := range arrivalOrderViolations(frames, ids, priorities, size, size/8) {
				t.Error(violation)
			}
		})
	}
}

// The order assertion must reject each way the scheduler could ignore part of
// a priority, not only pass the order it implements.
func TestStreamPriorityArrivalOrderControls(t *testing.T) {
	priorities := []streamPrioritySpec{{7, true}, {7, true}, {4, false}, {4, false}, {1, true}}
	ids := []uint64{2, 6, 10, 14, 18}
	const size, packet = 64 << 10, 1140
	type step struct {
		streams []int // served one packet each in turn
		bytes   uint64
	}
	all := func(streams ...int) step { return step{streams, size} }
	flight := step{[]int{0}, 10 * packet}
	for _, tc := range []struct {
		name  string
		steps []step
		ok    bool
	}{
		{"scheduled", []step{flight, all(4), all(2), all(3), all(0, 1)}, true},
		{"retransmission slack", []step{flight, {[]int{4}, size / 2}, {[]int{2, 3, 1}, 2 * packet}, all(4), all(2), all(3), all(0, 1)}, true},
		{"reversed", []step{all(0, 1), all(2), all(3), all(4)}, false},
		{"unprioritized", []step{all(0), all(1), all(2), all(3), all(4)}, false},
		{"urgent last", []step{flight, all(2), all(3), all(0, 1), all(4)}, false},
		{"urgent after one class", []step{flight, all(2), all(3), all(4), all(0, 1)}, false},
		{"background before middle class", []step{flight, all(4), all(0, 1), all(2), all(3)}, false},
		{"urgent shares turns", []step{flight, all(4, 2), all(3), all(0, 1)}, false},
		{"non-incremental pair shares turns", []step{flight, all(4), all(2, 3), all(0, 1)}, false},
		{"non-incremental pair by descending ID", []step{flight, all(4), all(3), all(2), all(0, 1)}, false},
		{"incremental pair in sequence", []step{flight, all(4), all(2), all(3), all(0), all(1)}, false},
		{"incremental pair in reverse sequence", []step{flight, all(4), all(2), all(3), all(1), all(0)}, false},
		{"incremental pair in large turns", []step{flight, all(4), all(2), all(3), {[]int{1}, size / 2}, {[]int{0}, size / 2}, all(1), all(0)}, false},
		{"empty", nil, false},
		{"truncated", []step{flight, all(4), all(2), all(3), {[]int{0, 1}, size / 2}}, false},
	} {
		var frames []streamArrival
		high := make([]uint64, len(ids))
		for _, step := range tc.steps {
			for active := true; active; {
				active = false
				for _, i := range step.streams {
					if high[i] < step.bytes {
						high[i] = min(high[i]+packet, step.bytes)
						frames = append(frames, streamArrival{ids[i], high[i]})
						active = true
					}
				}
			}
		}
		if violations := arrivalOrderViolations(frames, ids, priorities, size, size/8); (len(violations) == 0) != tc.ok {
			t.Errorf("%s: want accepted=%v, violations %q", tc.name, tc.ok, violations)
		}
	}
}
