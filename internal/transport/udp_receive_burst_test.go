package transport

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// The owner handles a burst of waiting datagrams before its next send pass,
// so one ACK covers the burst instead of one per pair of packets.
func TestUDPServerReceiveBurstSharesOneACK(t *testing.T) {
	c, peer := testPair(t, wire.Version1)
	drivePair(t, c, peer, false, false)
	ackNow(c)
	drain(t, c, time.Now())
	largest, _ := c.spaces[applicationSpace].received.Largest()
	sender, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	receiver, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := NewClientEndpoint(c, sender.LocalAddr().(*net.UDPAddr).AddrPort())
	if err != nil {
		receiver.Close()
		t.Fatal(err)
	}
	runtime, err := NewUDPServer(receiver, endpoint)
	if err != nil {
		receiver.Close()
		t.Fatal(err)
	}
	defer runtime.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// Hold the owner so the burst queues up behind it.
	entered, hold := make(chan struct{}), make(chan struct{})
	// Deferred after runtime.Close, so it runs first: a failure while the
	// owner is held must not leave Close waiting on it.
	release := sync.OnceFunc(func() { close(hold) })
	defer release()
	held := make(chan error, 1)
	go func() {
		_, err := runtime.invoke(ctx, func(*ServerEndpoint) error { close(entered); <-hold; return nil }, false)
		held <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	const burst = 6
	for i := uint64(1); i <= burst; i++ {
		packet := makePeerPacket(t, peer, applicationSpace, largest+i, wire.PingFrame{})
		if _, err := sender.WriteToUDP(packet, receiver.LocalAddr().(*net.UDPAddr)); err != nil {
			t.Fatal(err)
		}
	}
	for len(runtime.packets) < burst {
		select {
		case <-ctx.Done():
			t.Fatal("burst not queued", len(runtime.packets), ctx.Err())
		case <-time.After(time.Millisecond):
		}
	}
	release()
	if err := <-held; err != nil {
		t.Fatal(err)
	}
	// The first ACK must already cover the whole burst.
	if err := sender.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 65535)
	for {
		n, _, err := sender.ReadFromUDP(buffer)
		if err != nil {
			t.Fatal("no ACK", err)
		}
		for _, f := range framesIn(t, peer, buffer[:n]) {
			ack, ok := f.(wire.AckFrame)
			if !ok {
				continue
			}
			if got := ack.Ranges[0]; got.Largest != largest+burst || got.Smallest > largest+1 {
				t.Fatalf("first ACK covers %d..%d, want the whole burst %d..%d", got.Smallest, got.Largest, largest+1, largest+burst)
			}
			return
		}
	}
}
