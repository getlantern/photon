package interop

import (
	"fmt"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
)

// Bounded, failure-only packet evidence for intermittent handshake stalls.
// Keep only the public header prefix, never decrypted data or TLS secrets.
// Recording is shared by the relay goroutine and test owner.
type handshakeTrace struct {
	mu     sync.Mutex
	start  time.Time
	events [128]handshakeTraceEvent
	total  int
}
type handshakeTraceEvent struct {
	at        time.Duration
	direction string
	peer      netip.AddrPort
	size      int
	header    [6]byte
}

func newHandshakeTrace(t *testing.T) *handshakeTrace {
	h := &handshakeTrace{start: time.Now()}
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		h.mu.Lock()
		defer h.mu.Unlock()
		t.Logf("handshake trace: %d total events (last %d retained)", h.total, min(h.total, len(h.events)))
		for i := max(0, h.total-len(h.events)); i < h.total; i++ {
			e := h.events[i%len(h.events)]
			t.Logf("%s %s peer=%s size=%d header=%x", e.at, e.direction, e.peer, e.size, e.header[:min(e.size, len(e.header))])
		}
	})
	return h
}
func (h *handshakeTrace) packet(direction string, peer netip.AddrPort, data []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	e := handshakeTraceEvent{at: time.Since(h.start), direction: direction, peer: peer, size: len(data)}
	copy(e.header[:], data)
	h.events[h.total%len(h.events)] = e
	h.total++
}

func (h *handshakeTrace) AddProducer() qlogwriter.Recorder { return h }
func (h *handshakeTrace) SupportsSchemas(string) bool      { return true }
func (h *handshakeTrace) Close() error                     { return nil }
func (h *handshakeTrace) RecordEvent(event qlogwriter.Event) {
	switch e := event.(type) {
	case qlog.PacketDropped:
		h.packet(fmt.Sprintf("upstream-drop type=%v trigger=%v dcid=%v scid=%v", e.Header.PacketType, e.Trigger, e.Header.DestConnectionID, e.Header.SrcConnectionID), netip.AddrPort{}, nil)
	}
}
