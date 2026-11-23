package http3wire

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"testing/iotest"

	"github.com/getlantern/photon/internal/wire"
)

func controlFrame(typ uint64, payload ...byte) []byte {
	b, _ := AppendHeader(nil, Header{typ, uint64(len(payload))})
	return append(b, payload...)
}
func controlInput(frames ...[]byte) []byte { return bytes.Join(frames, nil) }
func controlReader(t *testing.T, b []byte, server bool) *ControlReader {
	t.Helper()
	r, err := NewControlReader(iotest.OneByteReader(bytes.NewReader(b)), server, 64, 8)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func TestControlStreamSequence(t *testing.T) {
	b := controlInput(controlFrame(Settings, 1, 0), controlFrame(33, 1, 2, 3), controlFrame(Goaway, 12), controlFrame(Goaway, 12), controlFrame(Goaway, 4), controlFrame(CancelPush, 0))
	r := controlReader(t, b, true)
	for i, want := range []ControlEvent{{Type: Settings, Settings: []Setting{{1, 0}}}, {Type: Goaway, ID: 12}, {Type: Goaway, ID: 12}, {Type: Goaway, ID: 4}, {Type: CancelPush}} {
		got, err := r.Next()
		if err != nil || got.Type != want.Type || got.ID != want.ID || len(got.Settings) != len(want.Settings) {
			t.Fatalf("event %d: %v %v", i, got, err)
		}
		if i == 0 && got.Settings[0] != want.Settings[0] {
			t.Fatal(got)
		}
	}
	_, err := r.Next()
	requireCode(t, err, ClosedCriticalStream)
}
func TestControlErrors(t *testing.T) {
	for _, tc := range []struct {
		name       string
		b          []byte
		server     bool
		successful int
		code       uint64
	}{
		{"empty", nil, false, 0, ClosedCriticalStream},
		{"unknown-first", controlFrame(33), false, 0, MissingSettings},
		{"data-first", controlFrame(Data), false, 0, MissingSettings},
		{"duplicate-settings", controlInput(controlFrame(Settings), controlFrame(Settings)), false, 1, FrameUnexpected},
		{"data-control", controlInput(controlFrame(Settings), controlFrame(Data)), false, 1, FrameUnexpected},
		{"server-maxpush", controlInput(controlFrame(Settings), controlFrame(MaxPushID, 1)), true, 1, FrameUnexpected},
		{"bad-goaway-id", controlInput(controlFrame(Settings), controlFrame(Goaway, 1)), true, 1, IDError},
		{"increasing-goaway", controlInput(controlFrame(Settings), controlFrame(Goaway, 4), controlFrame(Goaway, 8)), true, 2, IDError},
		{"decreasing-maxpush", controlInput(controlFrame(Settings), controlFrame(MaxPushID, 2), controlFrame(MaxPushID, 1)), false, 2, IDError},
		{"empty-id", controlInput(controlFrame(Settings), controlFrame(CancelPush)), false, 1, FrameError},
		{"extra-id", controlInput(controlFrame(Settings), controlFrame(Goaway, 0, 0)), false, 1, FrameError},
		{"truncated-body", []byte{4, 1}, false, 0, FrameError},
		{"truncated-header", []byte{4}, false, 0, FrameError},
		{"truncated-unknown", []byte{4, 0, 33, 1}, false, 1, FrameError},
		{"bad-settings", controlFrame(Settings, 1), false, 0, FrameError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := controlReader(t, tc.b, tc.server)
			for i := 0; i < tc.successful; i++ {
				if _, err := r.Next(); err != nil {
					t.Fatal(err)
				}
			}
			_, err := r.Next()
			requireCode(t, err, tc.code)
			_, again := r.Next()
			if again != err {
				t.Fatal("terminal error changed")
			}
		})
	}
}
func TestControlLimitsAndAbruptError(t *testing.T) {
	huge, _ := AppendHeader(nil, Header{Settings, wire.MaxVarint})
	r := controlReader(t, huge, false)
	_, err := r.Next()
	requireCode(t, err, ExcessiveLoad)
	if r.settings {
		t.Fatal("oversized settings committed")
	}
	r, _ = NewControlReader(bytes.NewReader(controlFrame(Settings, 1, 0, 6, 0)), false, 64, 1)
	_, err = r.Next()
	requireCode(t, err, ExcessiveLoad)
	if r.settings {
		t.Fatal("too many settings committed")
	}
	reset := errors.New("QUIC reset")
	r, _ = NewControlReader(io.MultiReader(bytes.NewReader(controlFrame(Settings)), brokenReader{reset}), false, 64, 8)
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err != reset {
		t.Fatalf("reset changed to %v", err)
	}
}
func TestClientPushLimitsAndGoaway(t *testing.T) {
	r := controlReader(t, controlInput(controlFrame(Settings), controlFrame(MaxPushID, 0), controlFrame(MaxPushID, 0), controlFrame(MaxPushID, 9), controlFrame(Goaway, 7), controlFrame(Goaway, 1)), false)
	for range 6 {
		if _, err := r.Next(); err != nil {
			t.Fatal(err)
		}
	}
}
func TestFramePlacement(t *testing.T) {
	for _, tc := range []struct {
		typ     uint64
		allowed [3]bool
	}{
		{Data, [3]bool{false, true, true}}, {Headers, [3]bool{false, true, true}},
		{CancelPush, [3]bool{true, false, false}}, {Settings, [3]bool{true, false, false}},
		{Goaway, [3]bool{true, false, false}}, {MaxPushID, [3]bool{true, false, false}},
		{PushPromise, [3]bool{false, true, false}}, {33, [3]bool{true, true, true}},
		{2, [3]bool{}}, {6, [3]bool{}}, {8, [3]bool{}}, {9, [3]bool{}},
	} {
		for _, server := range []bool{false, true} {
			for kind := ControlStream; kind <= PushStream; kind++ {
				allowed := tc.allowed[kind] && !(tc.typ == MaxPushID && server) && !(tc.typ == PushPromise && !server)
				err := ValidatePlacement(kind, server, tc.typ)
				if allowed {
					if err != nil {
						t.Fatal(tc.typ, kind, server, err)
					}
				} else {
					requireCode(t, err, FrameUnexpected)
				}
			}
		}
	}
}
func FuzzControl(f *testing.F) {
	for _, b := range [][]byte{{4, 0}, {4, 0, 7, 1, 0}, {33, 0}, {4, 2, 1, 0, 33, 0}} {
		f.Add(b, true)
	}
	f.Fuzz(func(t *testing.T, b []byte, server bool) {
		r, err := NewControlReader(bytes.NewReader(b), server, 64, 8)
		if err != nil {
			t.Fatal(err)
		}
		for {
			event, err := r.Next()
			if err != nil {
				if _, again := r.Next(); again != err {
					t.Fatal("unstable error")
				}
				return
			}
			if event.Type == Settings && len(event.Settings) > 8 {
				t.Fatal("unbounded settings")
			}
		}
	})
}
