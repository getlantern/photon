package transport

import "sync"

// resetReturnTarget records the source owner's selection before authentication
// is attempted elsewhere. Exactly one identity is set. The foreign owner must
// never inspect either connection state or the source router; it only completes
// the ticket. The source must revalidate this exact identity on receipt instead
// of choosing a replacement from a newer routing table.
type resetReturnTarget struct {
	conn  *Conn
	route *handoffRoute
}

type resetReturnResult struct {
	packet receivedUDP
	target resetReturnTarget
}

// resetReturnMailbox reserves space before a source sends a packet for foreign
// authentication. Pending results and queued returns together cannot exceed the
// capacity. Thus completing a reserved return never waits for the source owner,
// even when two owners are concurrently authenticating each other's packets.
// A dequeued result belongs to the source and no longer occupies mailbox space.
type resetReturnMailbox struct {
	mu       sync.Mutex
	closed   bool
	reserved int
	packets  chan resetReturnResult
}

type resetReturnTicket struct {
	mailbox *resetReturnMailbox
	target  resetReturnTarget
	done    bool // guarded by mailbox.mu
}

func newResetReturnMailbox() *resetReturnMailbox {
	return &resetReturnMailbox{packets: make(chan resetReturnResult, 64)}
}

// reserve runs before dispatch. Failure means the caller cannot request a
// return. Neither successful reservation nor cancellation transfers a buffer.
func (m *resetReturnMailbox) reserve(target resetReturnTarget) *resetReturnTicket {
	if (target.conn == nil) == (target.route == nil) {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.reserved+len(m.packets) >= cap(m.packets) {
		return nil
	}
	m.reserved++
	return &resetReturnTicket{mailbox: m, target: target}
}

// complete consumes the reservation once. It transfers ownership of p.buffer
// only when it returns true; otherwise the caller remains responsible for it.
// Authenticated packets never return for reset fallback, including packets that
// authenticate but subsequently cause a transport error. Invalid inputs consume
// the reservation without enqueueing. No connection state is inspected here.
func (t *resetReturnTicket) complete(p receivedUDP, authenticated bool) bool {
	m := t.mailbox
	m.mu.Lock()
	defer m.mu.Unlock()
	if t.done {
		return false
	}
	t.done = true
	m.reserved--
	if authenticated || m.closed || p.buffer == nil || p.err != nil {
		return false
	}
	// The reservation guarantees capacity; other producers cannot consume it.
	m.packets <- resetReturnResult{packet: p, target: t.target}
	return true
}

// cancel releases a reservation when dispatch fails or a destination discards
// queued work during shutdown. It is safe after completion or prior cancellation.
func (t *resetReturnTicket) cancel() {
	m := t.mailbox
	m.mu.Lock()
	defer m.mu.Unlock()
	if !t.done {
		t.done = true
		m.reserved--
	}
}

// close is called by the source after its receive loop exits. It releases queued
// buffers, but outstanding tickets retain their reservations until completion or
// cancellation. Late completions fail without taking buffer ownership. The
// channel stays open to avoid send/close races; consumers stop before this call.
func (m *resetReturnMailbox) close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	for {
		select {
		case result := <-m.packets:
			udpReadBuffers.Put(result.packet.buffer)
		default:
			return
		}
	}
}
