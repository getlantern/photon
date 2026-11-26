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
	return newSharedWANRelay(tb, server, newWANBottleneck(up), newWANBottleneck(down), 1)
}

// wanBottleneck is one direction of a link that several relays can share,
// so their clients' flows compete for its rate and queue.
type wanBottleneck struct {
	link  wanLink
	mu    sync.Mutex
	shape wanShaper
}

func newWANBottleneck(link wanLink) *wanBottleneck {
	return &wanBottleneck{link: link, shape: wanShaper{link: link}}
}

// admit admits a datagram of n bytes arriving now. It reads the clock under
// the lock, so relays sharing the bottleneck admit in time order.
func (b *wanBottleneck) admit(n int) (time.Time, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.shape.admit(time.Now(), n)
}

// newSharedWANRelay is a relay for one client over shared bottlenecks. Its
// random loss is drawn from seeds derived from seed, so relays sharing a
// link lose different packets.
func newSharedWANRelay(tb testing.TB, server string, up, down *wanBottleneck, seed uint64) *wanRelay {
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
	r.direction(0, up, 2*seed-1, func(b []byte) (int, error) {
		n, from, err := front.ReadFromUDP(b)
		if err == nil {
			r.client.Store(from)
		}
		return n, err
	}, func(b []byte) { back.Write(b) })
	r.direction(1, down, 2*seed, back.Read, func(b []byte) {
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

// wanShaper is a link's bottleneck: a datagram that arrives at now departs
// once the datagrams ahead of it have been transmitted, unless that backlog
// is longer than the link's queue, when it is dropped.
type wanShaper struct {
	link      wanLink
	busyUntil time.Time
}

// admit returns when a datagram of n bytes arriving at now finishes
// transmission, or false if the queue drops it.
func (s *wanShaper) admit(now time.Time, n int) (time.Time, bool) {
	if s.link.rate <= 0 {
		return now, true
	}
	departure := now
	if s.busyUntil.After(now) {
		departure = s.busyUntil
	}
	if departure.Sub(now) > s.link.queue {
		return time.Time{}, false
	}
	s.busyUntil = departure.Add(time.Duration(float64(n) / s.link.rate * float64(time.Second)))
	return s.busyUntil, true
}

// direction reads datagrams, applies the link, and delivers them in order from
// a second goroutine once their time comes. Departure times never decrease,
// even on a shared bottleneck, and the delay is fixed, so delivery order is
// arrival order.
func (r *wanRelay) direction(index int, bottleneck *wanBottleneck, seed uint64, read func([]byte) (int, error), write func([]byte)) {
	link := bottleneck.link
	ready := make(chan wanPacket, 1<<14)
	r.wg.Add(2)
	go func() {
		defer r.wg.Done()
		defer close(ready)
		rng := rand.New(rand.NewPCG(seed, seed))
		buf := make([]byte, 65536)
		for {
			n, err := read(buf)
			if err != nil {
				return
			}
			if n == 0 {
				continue
			}
			if link.loss > 0 && rng.Float64() < link.loss {
				r.dropped[index].Add(1)
				continue
			}
			departure, ok := bottleneck.admit(n)
			if !ok {
				r.dropped[index].Add(1)
				continue
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
	// A burst arrives before collect reads it; a kernel drop here would
	// escape the relay's drop count.
	if err := sink.SetReadBuffer(8 << 20); err != nil {
		t.Fatal(err)
	}
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

// The bottleneck admits a burst only while its backlog fits the queue,
// drops the rest, and admits again once the backlog has drained.
func TestWANShaperQueue(t *testing.T) {
	// 1000-byte datagrams take 10 ms each at 100 KB/s, so a 50 ms queue
	// admits a burst's first 6: the 6th waits exactly 50 ms.
	const size, rate = 1000, 100_000
	s := wanShaper{link: wanLink{rate: rate, queue: 50 * time.Millisecond}}
	start := time.Unix(0, 0)
	admitted := 0
	var last time.Time
	for range 20 {
		if at, ok := s.admit(start, size); ok {
			admitted++
			last = at
		}
	}
	if admitted != 6 {
		t.Fatal("admitted", admitted, "of a 20-datagram burst, want 6")
	}
	if want := start.Add(60 * time.Millisecond); !last.Equal(want) {
		t.Fatal("last departs at", last.Sub(start), "want", want.Sub(start))
	}
	// After 20 ms two have left, so two more fit.
	later, admitted := start.Add(20*time.Millisecond), 0
	for range 5 {
		if _, ok := s.admit(later, size); ok {
			admitted++
		}
	}
	if admitted != 2 {
		t.Fatal("admitted", admitted, "after 20 ms of drain, want 2")
	}
	if at, ok := (&wanShaper{}).admit(start, size); !ok || !at.Equal(start) {
		t.Fatal("unlimited link delayed or dropped a datagram", at, ok)
	}
}

// End to end, the relay paces admitted datagrams at the link rate and counts
// every one it drops.
func TestWANRelayBottleneckPacing(t *testing.T) {
	const size, rate = 1000, 100_000
	p := newRelayProbe(t, wanLink{rate: rate, queue: 50 * time.Millisecond})
	sent := p.send(t, 20, size)
	p.collect(t, 20, 300*time.Millisecond)
	if len(p.received) == 0 || len(p.received) == 20 {
		t.Fatal("received", len(p.received), "of a 20-datagram burst, want some dropped")
	}
	if dropped := p.relay.dropped[0].Load(); int(dropped)+len(p.received) != 20 {
		t.Fatal("dropped", dropped, "plus received", len(p.received), "is not 20")
	}
	// The last can't arrive before every admitted datagram has been
	// transmitted, counted from the first send. A late read only lengthens
	// the measured span.
	if span, min := p.at[len(p.at)-1].Sub(sent[0]), time.Duration(len(p.at))*size*time.Second/rate; span < min {
		t.Fatal("admitted datagrams arrived within", span, "want at least", min)
	}
}
