package interop

import (
	"encoding/binary"
	"math/rand/v2"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// wanLink shapes one direction of a simulated path: random loss, then a
// bottleneck of rate bytes per second (0 is unlimited) with a drop-tail queue
// holding at most queue of backlog, then a fixed one-way propagation delay.
// Loss is drawn first, for every packet, so the same packet indices are lost
// whatever the queue does; a lost packet uses no bottleneck time.
type wanLink struct {
	delay time.Duration
	loss  float64
	rate  float64
	queue time.Duration
}

type wanPacket struct {
	at   time.Time
	data []byte
}

// wanRelay is a UDP relay for one client that applies a wanLink in each
// direction. Losses are drawn from a fixed seed per direction, so every run
// drops the same packet indices; timing still varies between runs.
type wanRelay struct {
	front, back *net.UDPConn
	client      atomic.Pointer[net.UDPAddr]
	dropped     [2]atomic.Uint64
	wg          sync.WaitGroup
}

func newWANRelay(tb testing.TB, server string, up, down wanLink) *wanRelay {
	tb.Helper()
	remote, err := net.ResolveUDPAddr("udp", server)
	if err != nil {
		tb.Fatal(err)
	}
	front, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		tb.Fatal(err)
	}
	back, err := net.DialUDP("udp", nil, remote)
	if err != nil {
		front.Close()
		tb.Fatal(err)
	}
	for _, c := range []*net.UDPConn{front, back} {
		c.SetReadBuffer(8 << 20)
		c.SetWriteBuffer(8 << 20)
	}
	r := &wanRelay{front: front, back: back}
	r.direction(0, up, 1, func(b []byte) (int, error) {
		n, from, err := front.ReadFromUDP(b)
		if err == nil {
			r.client.Store(from)
		}
		return n, err
	}, func(b []byte) { back.Write(b) })
	r.direction(1, down, 2, back.Read, func(b []byte) {
		if to := r.client.Load(); to != nil {
			front.WriteToUDP(b, to)
		}
	})
	tb.Cleanup(r.close)
	return r
}

func (r *wanRelay) addr() string { return r.front.LocalAddr().String() }

func (r *wanRelay) close() {
	r.front.Close()
	r.back.Close()
	r.wg.Wait()
}

// direction reads datagrams, applies the link, and delivers them in order from
// a second goroutine once their time comes. Departure times never decrease and
// the delay is fixed, so delivery order is arrival order.
func (r *wanRelay) direction(index int, link wanLink, seed uint64, read func([]byte) (int, error), write func([]byte)) {
	ready := make(chan wanPacket, 1<<14)
	r.wg.Add(2)
	go func() {
		defer r.wg.Done()
		defer close(ready)
		rng := rand.New(rand.NewPCG(seed, seed))
		var busyUntil time.Time
		buf := make([]byte, 65536)
		for {
			n, err := read(buf)
			if err != nil {
				return
			}
			if n == 0 {
				continue
			}
			now := time.Now()
			if link.loss > 0 && rng.Float64() < link.loss {
				r.dropped[index].Add(1)
				continue
			}
			departure := now
			if link.rate > 0 {
				if busyUntil.After(now) {
					departure = busyUntil
				}
				if departure.Sub(now) > link.queue {
					r.dropped[index].Add(1)
					continue
				}
				departure = departure.Add(time.Duration(float64(n) / link.rate * float64(time.Second)))
				busyUntil = departure
			}
			select {
			case ready <- wanPacket{at: departure.Add(link.delay), data: append([]byte(nil), buf[:n]...)}:
			default:
				r.dropped[index].Add(1)
			}
		}
	}()
	go func() {
		defer r.wg.Done()
		for p := range ready {
			if wait := time.Until(p.at); wait > 0 {
				time.Sleep(wait)
			}
			write(p.data)
		}
	}()
}

// relayProbe sends numbered datagrams through a relay to a plain UDP socket
// and records what arrives, in order, with arrival times.
type relayProbe struct {
	relay    *wanRelay
	client   *net.UDPConn
	sink     *net.UDPConn
	received []uint32
	at       []time.Time
}

func newRelayProbe(t *testing.T, link wanLink) *relayProbe {
	t.Helper()
	sink, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sink.Close() })
	relay := newWANRelay(t, sink.LocalAddr().String(), link, wanLink{})
	client, err := net.DialUDP("udp", nil, relay.front.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return &relayProbe{relay: relay, client: client, sink: sink}
}

func (p *relayProbe) send(t *testing.T, n, size int) []time.Time {
	t.Helper()
	sent := make([]time.Time, n)
	b := make([]byte, size)
	for i := range n {
		binary.BigEndian.PutUint32(b, uint32(i))
		sent[i] = time.Now()
		if _, err := p.client.Write(b); err != nil {
			t.Fatal(err)
		}
	}
	return sent
}

// collect reads until want datagrams arrive or idle passes with none.
func (p *relayProbe) collect(t *testing.T, want int, idle time.Duration) {
	t.Helper()
	b := make([]byte, 65536)
	for len(p.received) < want {
		p.sink.SetReadDeadline(time.Now().Add(idle))
		n, err := p.sink.Read(b)
		if err != nil {
			return
		}
		if n >= 4 {
			p.received = append(p.received, binary.BigEndian.Uint32(b))
			p.at = append(p.at, time.Now())
		}
	}
}

// Every datagram arrives, in order, no sooner than the link's delay.
func TestWANRelayDelayAndOrder(t *testing.T) {
	const delay, n = 20 * time.Millisecond, 50
	p := newRelayProbe(t, wanLink{delay: delay})
	sent := p.send(t, n, 100)
	p.collect(t, n, time.Second)
	if len(p.received) != n {
		t.Fatal("received", len(p.received), "of", n)
	}
	for i, seq := range p.received {
		if seq != uint32(i) {
			t.Fatal("datagram", seq, "arrived at position", i)
		}
		if took := p.at[i].Sub(sent[i]); took < delay {
			t.Fatal("datagram", i, "took", took, "under the", delay, "delay")
		}
	}
}

// Seeded loss drops the same datagrams on every relay with the same link.
func TestWANRelaySeededLoss(t *testing.T) {
	const n = 400
	var runs [2][]uint32
	for r := range runs {
		p := newRelayProbe(t, wanLink{loss: 0.2})
		p.send(t, n, 100)
		p.collect(t, n, 200*time.Millisecond)
		if dropped := p.relay.dropped[0].Load(); int(dropped)+len(p.received) != n {
			t.Fatal("dropped", dropped, "plus received", len(p.received), "is not", n)
		}
		runs[r] = p.received
	}
	if len(runs[0]) == n || len(runs[0]) < n/2 {
		t.Fatal("20% loss delivered", len(runs[0]), "of", n)
	}
	if !slices.Equal(runs[0], runs[1]) {
		t.Fatal("same seed, different losses")
	}
}

// A burst into the bottleneck queues only as much as the queue's time bound
// holds and drops the rest, and admitted datagrams leave at the link rate.
func TestWANRelayBottleneckQueue(t *testing.T) {
	// 1000-byte datagrams take 10 ms each at 100 KB/s; a 50 ms queue admits
	// those that start within 50 ms of backlog: 6, or 7 if the burst spreads.
	const size, rate = 1000, 100_000
	p := newRelayProbe(t, wanLink{rate: rate, queue: 50 * time.Millisecond})
	p.send(t, 20, size)
	p.collect(t, 20, 300*time.Millisecond)
	if got := len(p.received); got < 6 || got > 7 {
		t.Fatal("admitted", got, "of a 20-datagram burst, want 6 or 7")
	}
	if dropped := p.relay.dropped[0].Load(); int(dropped)+len(p.received) != 20 {
		t.Fatal("dropped", dropped, "plus received", len(p.received), "is not 20")
	}
	// Serialization spaces arrivals: the last admitted datagram leaves at
	// least (admitted-1) transmit times after the first.
	if span, min := p.at[len(p.at)-1].Sub(p.at[0]), time.Duration(len(p.at)-1)*size*time.Second/rate; span < min*9/10 {
		t.Fatal("arrivals spanned", span, "want at least", min)
	}
}
