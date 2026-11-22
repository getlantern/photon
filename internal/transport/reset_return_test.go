package transport

import (
	"errors"
	"sync"
	"testing"
)

func TestResetReturnCapacity(t *testing.T) {
	m := newResetReturnMailbox()
	defer m.close()
	target := resetReturnTarget{conn: new(Conn)}
	tickets := make([]*resetReturnTicket, cap(m.packets))
	for i := range tickets {
		tickets[i] = m.reserve(target)
		if tickets[i] == nil {
			t.Fatalf("reservation %d rejected before capacity", i)
		}
	}
	if m.reserve(target) != nil {
		t.Fatal("pending reservations exceeded capacity")
	}
	// Converting reservations into returns must not create capacity. In
	// particular, a channel-only capacity check would admit another producer.
	for _, ticket := range tickets[:len(tickets)/2] {
		buffer := udpReadBuffers.Get().(*[65535]byte)
		if !ticket.complete(receivedUDP{buffer: buffer, data: buffer[:21]}, false) {
			t.Fatal("reserved return rejected")
		}
		if m.reserve(target) != nil {
			t.Fatal("queued returns plus pending reservations exceeded capacity")
		}
	}
	result := <-m.packets
	udpReadBuffers.Put(result.packet.buffer)
	replacement := m.reserve(target)
	if replacement == nil || m.reserve(target) != nil {
		t.Fatal("dequeue did not release exactly one slot")
	}
	replacement.cancel()
	replacement.cancel()
	for _, ticket := range tickets {
		ticket.cancel() // Includes already completed tickets.
	}
	if m.reserved != 0 || len(m.packets) != len(tickets)/2-1 {
		t.Fatalf("unexpected accounting: reserved=%d queued=%d", m.reserved, len(m.packets))
	}
}

func TestResetReturnOwnership(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		name := "local"
		if foreign {
			name = "foreign"
		}
		t.Run(name, func(t *testing.T) {
			m := newResetReturnMailbox()
			defer m.close()
			target := resetReturnTarget{conn: new(Conn)}
			if foreign {
				target = resetReturnTarget{route: new(handoffRoute)}
			}
			expected := target
			ticket := m.reserve(target)
			target = resetReturnTarget{} // Selection is copied, never reselected.
			buffer := udpReadBuffers.Get().(*[65535]byte)
			buffer[0] = 0x40
			packet := receivedUDP{buffer: buffer, data: buffer[:21], forwarded: true, route: new(handoffRoute)}
			if !ticket.complete(packet, false) {
				udpReadBuffers.Put(buffer)
				t.Fatal("unauthenticated packet did not transfer")
			}
			// A consumed ticket cannot enqueue another packet or release a slot.
			ticket.cancel()
			if ticket.complete(receivedUDP{}, false) || m.reserved != 0 || len(m.packets) != 1 {
				t.Fatal("ticket was not single use")
			}
			result := <-m.packets
			defer udpReadBuffers.Put(result.packet.buffer)
			if result.target != expected || result.packet.buffer != buffer ||
				len(result.packet.data) != 21 || &result.packet.data[0] != &buffer[0] ||
				result.packet.route != packet.route || !result.packet.forwarded {
				t.Fatal("return did not preserve target, original buffer, or provenance")
			}
		})
	}
}

func TestResetReturnSuppression(t *testing.T) {
	for _, mode := range []string{"authenticated", "socket-error", "missing-buffer", "cancelled", "closed"} {
		t.Run(mode, func(t *testing.T) {
			m := newResetReturnMailbox()
			defer m.close()
			ticket := m.reserve(resetReturnTarget{conn: new(Conn)})
			buffer := udpReadBuffers.Get().(*[65535]byte)
			defer udpReadBuffers.Put(buffer) // Suppression keeps caller ownership.
			packet := receivedUDP{buffer: buffer, data: buffer[:21]}
			switch mode {
			case "socket-error":
				packet.err = errors.New("read failed")
			case "missing-buffer":
				packet.buffer = nil
			case "cancelled":
				ticket.cancel()
			case "closed":
				m.close()
			}
			if ticket.complete(packet, mode == "authenticated") {
				t.Fatal("suppressed result transferred ownership")
			}
			if len(m.packets) != 0 || m.reserved != 0 {
				t.Fatal("suppression leaked queue space")
			}
		})
	}
}

func TestResetReturnClose(t *testing.T) {
	m := newResetReturnMailbox()
	target := resetReturnTarget{conn: new(Conn)}
	if m.reserve(resetReturnTarget{}) != nil || m.reserve(resetReturnTarget{conn: target.conn, route: new(handoffRoute)}) != nil {
		t.Fatal("accepted missing or ambiguous target identity")
	}
	queued, pending := m.reserve(target), m.reserve(target)
	buffer := udpReadBuffers.Get().(*[65535]byte)
	if !queued.complete(receivedUDP{buffer: buffer}, false) {
		t.Fatal("return rejected")
	}
	m.close()
	m.close()
	if len(m.packets) != 0 || m.reserved != 1 || m.reserve(target) != nil {
		t.Fatal("close did not drain queued work and retain outstanding reservation")
	}
	pending.cancel()
	if m.reserved != 0 {
		t.Fatal("late cancellation leaked reservation")
	}
	select {
	case _, ok := <-m.packets:
		if !ok {
			t.Fatal("close closed the channel")
		}
		t.Fatal("unexpected queued return")
	default:
	}
}

func TestResetReturnConcurrentShutdown(t *testing.T) {
	// Producers race source shutdown and destination queue cancellation. Each
	// producer has one buffer; either the mailbox or that producer releases it.
	for iteration := 0; iteration < 32; iteration++ {
		m := newResetReturnMailbox()
		start := make(chan struct{})
		var workers sync.WaitGroup
		for i := 0; i < cap(m.packets); i++ {
			ticket := m.reserve(resetReturnTarget{conn: new(Conn)})
			buffer := udpReadBuffers.Get().(*[65535]byte)
			workers.Add(2)
			go func() {
				defer workers.Done()
				<-start
				if !ticket.complete(receivedUDP{buffer: buffer, data: buffer[:21]}, false) {
					udpReadBuffers.Put(buffer)
				}
			}()
			go func() {
				defer workers.Done()
				<-start
				ticket.cancel()
			}()
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			m.close()
		}()
		close(start)
		workers.Wait()
		if m.reserved != 0 || len(m.packets) != 0 {
			t.Fatalf("shutdown leaked work: reserved=%d queued=%d", m.reserved, len(m.packets))
		}
	}
}
