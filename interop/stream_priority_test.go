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

// arrivedDuring reports how many new bytes of stream other the peer received
// between the first and the last new byte of stream id.
func arrivedDuring(frames []streamArrival, id, other, size uint64) (n uint64) {
	high := map[uint64]uint64{}
	for _, f := range frames {
		fresh := f.end > high[f.id]
		if fresh && f.id == other && high[id] > 0 && high[id] < size {
			n += f.end - high[f.id]
		}
		if fresh {
			high[f.id] = f.end
		}
	}
	return n
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
			priorities := []struct {
				urgency     int8
				incremental bool
			}{{7, true}, {7, true}, {4, false}, {4, false}, {1, true}}
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
			id := func(i int) uint64 { return uint64(streams[i].StreamID()) }
			// Retransmissions legitimately precede fresh data of any priority,
			// so allow a few packets rather than requiring exactly zero.
			const slack = size / 8
			for _, order := range [][2]int{{4, 0}, {4, 1}, {4, 2}, {4, 3}, {2, 0}, {2, 1}, {3, 0}, {3, 1}, {2, 3}} {
				if n := arrivedDuring(frames, id(order[0]), id(order[1]), size); n > slack {
					t.Errorf("stream %d (%+v) yielded %d bytes to stream %d (%+v)", order[0], priorities[order[0]], n, order[1], priorities[order[1]])
				}
			}
			// The first stream's head start is at most the initial congestion
			// window, after which the incremental pair must alternate.
			if n := arrivedDuring(frames, id(1), id(0), size); n < size/4 {
				t.Errorf("incremental streams did not share turns: %d bytes interleaved", n)
			}
		})
	}
}
