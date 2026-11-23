package http3wire

import (
	"bytes"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestPriorityUpdateControl(t *testing.T) {
	for _, typ := range []uint64{PriorityUpdateRequest, PriorityUpdatePush} {
		for _, id := range []uint64{0, 4, 64, 16384, wire.MaxVarint - 3} {
			payload, _ := wire.AppendVarint(nil, id)
			payload = append(payload, []byte("u=1, i")...)
			r := controlReader(t, controlInput(controlFrame(Settings), controlFrame(typ, payload...), controlFrame(Goaway, 0)), false)
			if _, err := r.Next(); err != nil {
				t.Fatal(err)
			}
			event, err := r.Next()
			if err != nil || event.Type != typ || event.ID != id || !event.PriorityValid || event.Priority != (Priority{Urgency: 1, Incremental: true}) {
				t.Fatal(event, err)
			}
			if next, err := r.Next(); err != nil || next.Type != Goaway {
				t.Fatal("lost frame boundary", next, err)
			}
		}
	}
}

func TestPriorityUpdateErrors(t *testing.T) {
	for _, test := range []struct {
		name    string
		typ     uint64
		payload []byte
		server  bool
		code    uint64
	}{
		{"empty", PriorityUpdateRequest, nil, false, FrameError},
		{"truncated", PriorityUpdateRequest, []byte{0x40}, false, FrameError},
		{"server request", PriorityUpdateRequest, []byte{0}, true, FrameUnexpected},
		{"server push", PriorityUpdatePush, []byte{0}, true, FrameUnexpected},
		{"server bidi", PriorityUpdateRequest, []byte{1}, false, IDError},
		{"client uni", PriorityUpdateRequest, []byte{2}, false, IDError},
		{"server uni", PriorityUpdateRequest, []byte{3}, false, IDError},
		{"budget", PriorityUpdateRequest, make([]byte, 4105), false, ExcessiveLoad},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := controlReader(t, controlInput(controlFrame(Settings), controlFrame(test.typ, test.payload...)), test.server)
			if _, err := r.Next(); err != nil {
				t.Fatal(err)
			}
			_, err := r.Next()
			requireCode(t, err, test.code)
			if _, again := r.Next(); again != err {
				t.Fatal("error not sticky")
			}
		})
	}
	for _, value := range []string{"u=0, broken=", "u=2, i=?2"} {
		r := controlReader(t, controlInput(controlFrame(Settings), controlFrame(PriorityUpdateRequest, append([]byte{0}, value...)...)), false)
		r.Next()
		event, err := r.Next()
		if err != nil || event.PriorityValid {
			t.Fatal(event, err)
		}
	}
	// An attacker-supplied giant length is rejected without consuming its body.
	header, _ := AppendHeader(nil, Header{Type: PriorityUpdateRequest, Length: wire.MaxVarint})
	r := controlReader(t, append(controlFrame(Settings), header...), false)
	r.Next()
	_, err := r.Next()
	requireCode(t, err, ExcessiveLoad)
}

func TestPriorityUpdatePlacement(t *testing.T) {
	for _, typ := range []uint64{PriorityUpdateRequest, PriorityUpdatePush} {
		for _, kind := range []StreamKind{ControlStream, RequestStream, PushStream} {
			for _, server := range []bool{false, true} {
				err := ValidatePlacement(kind, server, typ)
				if kind == ControlStream && !server {
					if err != nil {
						t.Fatal(err)
					}
				} else {
					requireCode(t, err, FrameUnexpected)
				}
			}
		}
	}
}

func FuzzPriorityUpdateControl(f *testing.F) {
	f.Add([]byte{0, 'u', '=', '1'}, false)
	f.Add([]byte{0xff}, true)
	f.Fuzz(func(t *testing.T, payload []byte, server bool) {
		if len(payload) > 8192 {
			return
		}
		data := controlInput(controlFrame(Settings), controlFrame(PriorityUpdateRequest, payload...))
		r, err := NewControlReader(bytes.NewReader(data), server, 64, 8)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = r.Next(); err != nil {
			t.Fatal(err)
		}
		event, err := r.Next()
		if err == nil && (server || event.ID%4 != 0 || event.Priority.Urgency < 0 || event.Priority.Urgency > 7) {
			t.Fatal(event)
		}
	})
}
