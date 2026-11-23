package http3conn

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"

	quic "github.com/getlantern/photon"
	h3 "github.com/getlantern/photon/internal/http3wire"
)

type trackedPriorityStream struct {
	*priorityRequestStream
	id quic.StreamID
}

func (s *trackedPriorityStream) StreamID() quic.StreamID { return s.id }
func trackedStream(id uint64) *trackedPriorityStream {
	return &trackedPriorityStream{priorityRequestStream: &priorityRequestStream{serveStream: &serveStream{}}, id: quic.StreamID(id)}
}
func priorityEvent(id uint64, u int8, i bool) h3.ControlEvent {
	return h3.ControlEvent{Type: h3.PriorityUpdateRequest, ID: id, Priority: h3.Priority{Urgency: u, Incremental: i}, PriorityValid: true}
}

func TestRequestPrioritiesLifecycle(t *testing.T) {
	p, err := NewRequestPriorities(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Update(priorityEvent(0, 7, false)); err != nil {
		t.Fatal(err)
	}
	if err := p.Update(priorityEvent(0, 1, true)); err != nil {
		t.Fatal(err)
	}
	stream := trackedStream(0)
	if err := p.register(stream); err != nil {
		t.Fatal(err)
	}
	p.header(stream, h3.Priority{Urgency: 6})
	if stream.priority != (h3.Priority{Urgency: 1, Incremental: true}) || len(p.pending) != 0 {
		t.Fatal("early update overridden by header", stream.priority)
	}
	// Updates replace the complete value: omitted i uses false, not the old true.
	if err := p.Update(priorityEvent(0, 3, false)); err != nil {
		t.Fatal(err)
	}
	invalid := priorityEvent(0, 0, true)
	invalid.PriorityValid = false
	if err := p.Update(invalid); err != nil {
		t.Fatal(err)
	}
	if stream.priority != (h3.Priority{Urgency: 3}) {
		t.Fatal("malformed signal replaced valid update", stream.priority)
	}
	next := trackedStream(4)
	if err := p.register(next); err != nil {
		t.Fatal(err)
	}
	p.header(next, h3.Priority{Urgency: 5, Incremental: true})
	if next.priority != (h3.Priority{Urgency: 5, Incremental: true}) {
		t.Fatal(next.priority)
	}
	if err := p.Update(priorityEvent(0, 0, true)); err != nil {
		t.Fatal(err)
	}
	if stream.priority.Urgency != 0 {
		t.Fatal("watermark hid active older request")
	}
	p.finish(0)
	p.finish(4)
	for i := 0; i < 100; i++ {
		if err := p.Update(priorityEvent(0, 7, false)); err != nil {
			t.Fatal(err)
		}
	}
	if len(p.active) != 0 || len(p.pending) != 0 {
		t.Fatal("completed request retained update state")
	}
}

func TestRequestPrioritiesBounds(t *testing.T) {
	p, _ := NewRequestPriorities(1)
	if err := p.Update(priorityEvent(4, 1, true)); err != nil {
		t.Fatal(err)
	}
	if err := p.Update(priorityEvent(4, 2, false)); err != nil {
		t.Fatal("replacement consumed capacity", err)
	}
	var protocol *h3.Error
	if err := p.Update(priorityEvent(8, 0, false)); !errors.As(err, &protocol) || protocol.Code != h3.ExcessiveLoad {
		t.Fatal(err)
	}
	if len(p.pending) != 1 {
		t.Fatal("budget exceeded")
	}
	if err := p.register(trackedStream(4)); err != nil {
		t.Fatal(err)
	}
	if err := p.register(trackedStream(8)); !errors.As(err, &protocol) || protocol.Code != h3.ExcessiveLoad {
		t.Fatal(err)
	}
	p.finish(4)
	if err := p.register(trackedStream(8)); err != nil {
		t.Fatal(err)
	}
	if err := p.register(trackedStream(4)); err == nil {
		t.Fatal("out-of-order acceptance")
	}
	if err := p.Update(priorityEvent(3, 0, false)); !errors.As(err, &protocol) || protocol.Code != h3.IDError {
		t.Fatal(err)
	}
}

func TestRequestPrioritiesHeaderRace(t *testing.T) {
	for n := 0; n < 50; n++ {
		p, _ := NewRequestPriorities(1)
		stream := trackedStream(0)
		if err := p.register(stream); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); p.header(stream, h3.Priority{Urgency: 7}) }()
		go func() {
			defer wg.Done()
			if err := p.Update(priorityEvent(0, 0, true)); err != nil {
				t.Error(err)
			}
		}()
		wg.Wait()
		if stream.priority != (h3.Priority{Urgency: 0, Incremental: true}) {
			t.Fatal(stream.priority)
		}
	}
}

func TestRequestPrioritiesServeHeaders(t *testing.T) {
	// Exercise the ServeRequest hook as well as the standalone tracker ordering.
	p, _ := NewRequestPriorities(1)
	stream := trackedStream(0)
	stream.source = bytes.NewReader(readerHeaders(t, append(requestFields("/"), fs("priority", "u=7"))...))
	if err := p.Update(priorityEvent(0, 0, true)); err != nil {
		t.Fatal(err)
	}
	if err := p.register(stream); err != nil {
		t.Fatal(err)
	}
	config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if stream.priority.Urgency != 0 || !stream.priority.Incremental {
			t.Error(stream.priority)
		}
		w.WriteHeader(204)
	}))
	config.Priorities = p
	if err := ServeRequest(context.Background(), stream, config); err != nil {
		t.Fatal(err)
	}
}
