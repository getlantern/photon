package interop

import (
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// wanLink shapes one direction of a simulated path: a bottleneck of rate
// bytes per second (0 is unlimited) with a drop-tail queue holding at most
// queue of backlog, random loss, then a fixed one-way propagation delay.
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
