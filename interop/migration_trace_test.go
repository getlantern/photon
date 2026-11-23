package interop

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go/qlog"
	"github.com/quic-go/quic-go/qlogwriter"
)

// A bounded, test-only diagnostic. No packet payloads or key material are kept.
// Events are formatted immediately because qlog frame backing storage may be reused.
type migrationTrace struct {
	mu    sync.Mutex
	start time.Time
	ring  [512]string
	next  uint64
}

func (t *migrationTrace) AddProducer() qlogwriter.Recorder { return t }
func (t *migrationTrace) SupportsSchemas(string) bool      { return true }
func (t *migrationTrace) Close() error                     { return nil }
func (t *migrationTrace) note(format string, args ...any) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.start.IsZero() {
		t.start = time.Now()
	}
	t.ring[t.next%uint64(len(t.ring))] = fmt.Sprintf("%s %s", time.Since(t.start).Round(time.Microsecond), fmt.Sprintf(format, args...))
	t.next++
}
func (t *migrationTrace) packet(direction, from, to string, data []byte, dropped bool) {
	if t != nil {
		t.note("relay %s %s -> %s bytes=%d checksum=%08x dropped=%v", direction, from, to, len(data), qlog.CalculateDatagramPayloadChecksum(data), dropped)
	}
}
func (t *migrationTrace) RecordEvent(event qlogwriter.Event) {
	var direction string
	var header qlog.PacketHeader
	var frames []qlog.Frame
	var checksum qlog.DatagramPayloadChecksum
	switch event := event.(type) {
	case qlog.PacketSent:
		direction, header, frames, checksum = "sent", event.Header, event.Frames, event.DatagramPayloadChecksum
	case qlog.PacketReceived:
		direction, header, frames, checksum = "received", event.Header, event.Frames, event.DatagramPayloadChecksum
	case qlog.PacketLost:
		t.note("upstream lost type=%v pn=%d reason=%v", event.Header.PacketType, event.Header.PacketNumber, event.Trigger)
		return
	default:
		return
	}
	if header.PacketType != qlog.PacketType1RTT {
		return
	}
	var description strings.Builder
	for _, wrapped := range frames {
		switch frame := wrapped.Frame.(type) {
		case *qlog.PathChallengeFrame:
			fmt.Fprintf(&description, " challenge=%x", frame.Data)
		case *qlog.PathResponseFrame:
			fmt.Fprintf(&description, " response=%x", frame.Data)
		default:
			fmt.Fprintf(&description, " %T", wrapped.Frame)
		}
	}
	t.note("upstream %s pn=%d checksum=%08x frames:%s", direction, header.PacketNumber, checksum, description.String())
}
func (t *migrationTrace) snapshot() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	start := uint64(0)
	if t.next > uint64(len(t.ring)) {
		start = t.next - uint64(len(t.ring))
	}
	lines := make([]string, 0, t.next-start)
	for i := start; i < t.next; i++ {
		lines = append(lines, t.ring[i%uint64(len(t.ring))])
	}
	return strings.Join(lines, "\n")
}
