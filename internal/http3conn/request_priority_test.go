package http3conn

import (
	"bytes"
	"context"
	"net/http"
	"testing"

	h3 "github.com/getlantern/photon/internal/http3wire"
)

type priorityRequestStream struct {
	*serveStream
	priority h3.Priority
	calls    int
}

func (s *priorityRequestStream) SetPriority(u int8, i bool) {
	s.priority = h3.Priority{Urgency: u, Incremental: i}
	s.calls++
}

func TestServeRequestPriority(t *testing.T) {
	for _, test := range []struct {
		name   string
		values []string
		want   h3.Priority
	}{
		{"default", nil, h3.Priority{Urgency: 3}},
		{"explicit", []string{"u=0, i"}, h3.Priority{Urgency: 0, Incremental: true}},
		{"combined", []string{"u=1", "i"}, h3.Priority{Urgency: 1, Incremental: true}},
		{"malformed", []string{"u=0", "invalid="}, h3.Priority{Urgency: 3}},
		{"unknown", []string{"u=6, x=(1 2);y"}, h3.Priority{Urgency: 6}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fields := requestFields("/priority")
			for _, value := range test.values {
				fields = append(fields, fs("priority", value))
			}
			stream := &priorityRequestStream{serveStream: &serveStream{source: bytes.NewReader(readerHeaders(t, fields...))}}
			called := false
			config := serveConfig(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				if stream.calls != 1 || stream.priority != test.want {
					t.Error("priority unavailable at dispatch", stream.priority, stream.calls)
				}
				w.WriteHeader(204)
			}))
			if err := ServeRequest(context.Background(), stream, config); err != nil {
				t.Fatal(err)
			}
			if !called {
				t.Fatal("handler not dispatched")
			}
		})
	}
}

type priorityDatagramTransport struct {
	*datagramTestStream
	priority h3.Priority
}

func (s *priorityDatagramTransport) SetPriority(u int8, i bool) {
	s.priority = h3.Priority{Urgency: u, Incremental: i}
}
func TestDatagramStreamPriorityForwarding(t *testing.T) {
	raw := &priorityDatagramTransport{datagramTestStream: &datagramTestStream{}}
	wrapped := &DatagramStream{DatagramTransportStream: raw}
	wrapped.SetPriority(2, true)
	if raw.priority != (h3.Priority{Urgency: 2, Incremental: true}) {
		t.Fatal(raw.priority)
	}
}
