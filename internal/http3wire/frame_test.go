package http3wire

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"testing/iotest"

	"github.com/getlantern/photon/internal/wire"
)

func requireCode(t *testing.T, err error, code uint64) {
	t.Helper()
	var protocol *Error
	if !errors.As(err, &protocol) || protocol.Code != code {
		t.Fatalf("error=%v want code=%x", err, code)
	}
}

func TestFragmentedFrames(t *testing.T) {
	// A nonminimal unknown type/length, payload, empty DATA, then HEADERS.
	input := []byte{0x40, 0x21, 0x40, 2, 'a', 'b', 0, 0, 1, 1, 'c'}
	for _, source := range []io.Reader{bytes.NewReader(input), iotest.OneByteReader(bytes.NewReader(input)), iotest.DataErrReader(bytes.NewReader(input))} {
		r := NewReader(source)
		for i, want := range []Header{{33, 2}, {Data, 0}, {Headers, 1}} {
			got, err := r.Next()
			if err != nil || got != want {
				t.Fatalf("header=%v error=%v want=%v", got, err, want)
			}
			if want.Length != 0 {
				if _, err := r.Next(); !errors.Is(err, ErrPayloadPending) {
					t.Fatal(err)
				}
			}
			payload, err := io.ReadAll(r)
			if err != nil || string(payload) != []string{"ab", "", "c"}[i] {
				t.Fatalf("payload=%q error=%v", payload, err)
			}
		}
		if _, err := r.Next(); !errors.Is(err, io.EOF) {
			t.Fatal(err)
		}
	}
}
func TestTruncatedFrames(t *testing.T) {
	for _, input := range [][]byte{{0x40}, {0}, {0, 0x40}, {0, 2, 'x'}, {0, 1}} {
		r := NewReader(bytes.NewReader(input))
		_, err := r.Next()
		if err == nil {
			_, err = io.ReadAll(r)
		}
		requireCode(t, err, FrameError)
		_, next := r.Next()
		if next != err {
			t.Fatalf("terminal error changed: %v => %v", err, next)
		}
	}
}
func TestFrameLimitsAndReservedTypes(t *testing.T) {
	for _, typ := range []byte{2, 6, 8, 9} {
		_, err := NewReader(bytes.NewReader([]byte{typ, 0})).Next()
		requireCode(t, err, FrameUnexpected)
	}
	// The largest legal frame length is accepted without allocation. A truncated
	// body then fails rather than being mistaken for a clean stream end.
	encoded, err := AppendHeader(nil, Header{wire.MaxVarint, wire.MaxVarint})
	if err != nil {
		t.Fatal(err)
	}
	r := NewReader(bytes.NewReader(encoded))
	h, err := r.Next()
	if err != nil || h.Length != wire.MaxVarint {
		t.Fatalf("%v: %v", h, err)
	}
	_, err = r.Read(make([]byte, 1))
	requireCode(t, err, FrameError)
	for _, h := range []Header{{wire.MaxVarint + 1, 0}, {0, wire.MaxVarint + 1}} {
		dst := []byte{99}
		got, err := AppendHeader(dst, h)
		if !errors.Is(err, wire.ErrVarintRange) || !bytes.Equal(got, dst) {
			t.Fatalf("%x: %v", got, err)
		}
	}
}

type brokenReader struct{ err error }

func (r brokenReader) Read([]byte) (int, error) { return 0, r.err }
func TestAbruptStreamError(t *testing.T) {
	reset := errors.New("stream reset")
	r := NewReader(io.MultiReader(bytes.NewReader([]byte{0, 5, 'x'}), brokenReader{reset}))
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(r)
	if string(b) != "x" || err != reset {
		t.Fatalf("%q: %v", b, err)
	}
}
func TestIdentifierPayload(t *testing.T) {
	if value, err := ParseID([]byte{0x40, 1}); err != nil || value != 1 {
		t.Fatalf("%d: %v", value, err)
	}
	for _, b := range [][]byte{nil, {0x40}, {0, 0}} {
		_, err := ParseID(b)
		requireCode(t, err, FrameError)
	}
}

func TestSettings(t *testing.T) {
	settings := []Setting{{QPACKMaxTableCapacity, 4096}, {MaxFieldSectionSize, 65536}, {QPACKBlockedStreams, 8}, {0x21, 9}, {0, 0}}
	encoded, err := AppendSettings(nil, settings)
	if err != nil {
		t.Fatal(err)
	}
	r := NewReader(iotest.OneByteReader(bytes.NewReader(encoded)))
	h, err := r.Next()
	if err != nil || h.Type != Settings {
		t.Fatalf("%v: %v", h, err)
	}
	payload, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseSettings(payload, len(settings))
	if err != nil || len(parsed) != len(settings) {
		t.Fatalf("%v: %v", parsed, err)
	}
	for i := range settings {
		if parsed[i] != settings[i] {
			t.Fatal(parsed)
		}
	}
	for _, tc := range []struct {
		b    []byte
		code uint64
	}{
		{[]byte{1}, FrameError}, {[]byte{1, 0x40}, FrameError},
		{[]byte{1, 0, 1, 1}, SettingsError}, {[]byte{33, 0, 0x40, 33, 1}, SettingsError},
		{[]byte{2, 0}, SettingsError}, {[]byte{3, 0}, SettingsError},
		{[]byte{4, 0}, SettingsError}, {[]byte{5, 0}, SettingsError},
	} {
		_, err := ParseSettings(tc.b, 10)
		requireCode(t, err, tc.code)
	}
	if _, err := ParseSettings(payload, len(settings)-1); err == nil {
		t.Fatal("entry cap ignored")
	} else {
		requireCode(t, err, ExcessiveLoad)
	}
	for _, invalid := range [][]Setting{{{1, 0}, {1, 1}}, {{5, 0}}, {{1, wire.MaxVarint + 1}}} {
		dst := []byte{99}
		got, err := AppendSettings(dst, invalid)
		if err == nil || !bytes.Equal(got, dst) {
			t.Fatalf("invalid settings changed destination: %x %v", got, err)
		}
	}
	if got, err := AppendSettings(nil, nil); err != nil || !bytes.Equal(got, []byte{4, 0}) {
		t.Fatalf("%x: %v", got, err)
	}
}

func FuzzFrames(f *testing.F) {
	for _, b := range [][]byte{{0, 1, 'a'}, {4, 2, 1, 0}, {0x40}, {33, 0, 0, 0}} {
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		r := NewReader(bytes.NewReader(b))
		for {
			h, err := r.Next()
			if err != nil {
				return
			}
			// The fuzzer input bounds available payload; the frame length never does.
			payload, err := io.ReadAll(r)
			if err != nil {
				return
			}
			if uint64(len(payload)) != h.Length {
				t.Fatal("payload escaped frame boundary")
			}
			if h.Type == Settings {
				settings, err := ParseSettings(payload, 64)
				if err == nil {
					encoded, err := AppendSettings(nil, settings)
					if err != nil {
						t.Fatal(err)
					}
					round := NewReader(bytes.NewReader(encoded))
					if _, err := round.Next(); err != nil {
						t.Fatal(err)
					}
					canonical, err := io.ReadAll(round)
					if err != nil {
						t.Fatal(err)
					}
					again, err := ParseSettings(canonical, 64)
					if err != nil || len(again) != len(settings) {
						t.Fatal("settings round trip")
					}
					for i := range again {
						if again[i] != settings[i] {
							t.Fatal("setting changed")
						}
					}
				}
			}
		}
	})
}
