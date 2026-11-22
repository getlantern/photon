package http3wire

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"testing/iotest"

	"github.com/getlantern/photon/internal/wire"
)

func TestHTTPDatagramEncoding(t *testing.T) {
	for _, tc := range []struct {
		data    []byte
		id      uint64
		payload []byte
	}{
		{[]byte{0}, 0, nil}, {[]byte{1, 0xaa, 0xbb}, 4, []byte{0xaa, 0xbb}},
		{[]byte{0x40, 1, 7}, 4, []byte{7}}, // Nonminimal integer is legal.
		{[]byte{0xcf, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, wire.MaxVarint - 3, nil},
	} {
		id, p, err := ParseDatagram(tc.data)
		if err != nil || id != tc.id || !bytes.Equal(p, tc.payload) {
			t.Fatal(id, p, err)
		}
		encoded, err := AppendDatagram(nil, id, p)
		if err != nil {
			t.Fatal(err)
		}
		again, body, err := ParseDatagram(encoded)
		if err != nil || again != id || !bytes.Equal(body, p) {
			t.Fatal("round trip", err)
		}
	}
	for _, data := range [][]byte{nil, {0x40}, {0x80, 0, 0}, {0xd0, 0, 0, 0, 0, 0, 0, 0}} {
		_, _, err := ParseDatagram(data)
		var e *Error
		if !errors.As(err, &e) || e.Code != DatagramError {
			t.Fatal("invalid datagram", err)
		}
	}
	for _, id := range []uint64{1, 2, 3, wire.MaxVarint, 1 << 62} {
		dst := []byte{99}
		got, err := AppendDatagram(dst, id, nil)
		if err == nil || !bytes.Equal(got, dst) {
			t.Fatal("nontransactional invalid ID")
		}
	}
}

func TestCapsuleStreaming(t *testing.T) {
	// Type 2 is forbidden as an HTTP/3 frame but is a legal unknown capsule.
	data := []byte{2, 3, 7, 8, 9, 0x40, 0, 0x40, 0, 0, 2, 4, 5}
	r := NewCapsuleReader(iotest.OneByteReader(bytes.NewReader(data)))
	h, err := r.Next()
	if err != nil || h != (Header{2, 3}) {
		t.Fatal(h, err)
	}
	if _, err := r.Next(); !errors.Is(err, ErrPayloadPending) {
		t.Fatal(err)
	}
	if n, err := io.Copy(io.Discard, r); err != nil || n != 3 {
		t.Fatal(n, err)
	}
	h, err = r.Next()
	if err != nil || h != (Header{0, 0}) {
		t.Fatal(h, err)
	}
	h, err = r.Next()
	if err != nil || h != (Header{0, 2}) {
		t.Fatal(h, err)
	}
	p, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(p, []byte{4, 5}) {
		t.Fatal(p, err)
	}
	if _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	// Every incomplete prefix of a capsule reports request-scoped truncation.
	full := []byte{0x40, 0, 0x40, 3, 1, 2, 3}
	for n := 1; n < len(full); n++ {
		r := NewCapsuleReader(bytes.NewReader(full[:n]))
		_, err := r.Next()
		if err == nil {
			_, err = io.Copy(io.Discard, r)
		}
		var e *Error
		if !errors.As(err, &e) || e.Code != 0x10e {
			t.Fatalf("prefix %d: %v", n, err)
		}
	}
	huge, _ := AppendCapsuleHeader(nil, DatagramCapsule, wire.MaxVarint)
	r = NewCapsuleReader(bytes.NewReader(huge))
	if h, err := r.Next(); err != nil || h.Length != wire.MaxVarint {
		t.Fatal(h, err)
	}
	if _, err := io.Copy(io.Discard, r); err == nil {
		t.Fatal("missing huge payload accepted")
	}
}

func TestDatagramSettingBounds(t *testing.T) {
	for _, v := range []uint64{0, 1, 2, wire.MaxVarint} {
		payload, _ := wire.AppendVarint(nil, H3Datagram)
		payload, _ = wire.AppendVarint(payload, v)
		_, err := ParseSettings(payload, 1)
		dst := []byte{77}
		out, encodeErr := AppendSettings(dst, []Setting{{H3Datagram, v}})
		if v <= 1 {
			if err != nil || encodeErr != nil {
				t.Fatal(err, encodeErr)
			}
			continue
		}
		var e *Error
		if !errors.As(err, &e) || e.Code != SettingsError || encodeErr == nil || !bytes.Equal(dst, out) {
			t.Fatal("invalid setting accepted", v, err, encodeErr)
		}
	}
}

func FuzzHTTPDatagram(f *testing.F) {
	f.Add([]byte{1, 2, 3})
	f.Add([]byte{0xd0, 0, 0, 0, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, data []byte) {
		id, p, err := ParseDatagram(data)
		if err != nil {
			return
		}
		encoded, err := AppendDatagram(nil, id, p)
		if err != nil {
			t.Fatal(err)
		}
		other, body, err := ParseDatagram(encoded)
		if err != nil || other != id || !bytes.Equal(body, p) {
			t.Fatal("round trip")
		}
	})
}

func FuzzCapsuleReader(f *testing.F) {
	f.Add([]byte{0, 0, 2, 2, 7, 8})
	f.Add([]byte{0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewCapsuleReader(iotest.OneByteReader(bytes.NewReader(data)))
		for i := 0; ; i++ {
			if i > len(data)/2 {
				t.Fatal("capsule parser made no progress")
			}
			h, err := r.Next()
			if err != nil {
				return
			}
			n, err := io.Copy(io.Discard, r)
			if err != nil {
				return
			}
			if uint64(n) != h.Length {
				t.Fatal("payload boundary mismatch")
			}
		}
	})
}

func TestPayloadReaderTerminalErrors(t *testing.T) {
	interrupted := errors.New("transport interrupted")
	for _, capsule := range []bool{false, true} {
		for _, terminal := range []error{io.EOF, io.ErrUnexpectedEOF, interrupted} {
			for _, partial := range []bool{false, true} {
				source := io.MultiReader(bytes.NewReader([]byte{0, 2}), &terminalPayloadReader{data: []byte{1, 2}, err: terminal, partial: partial})
				reader := NewReader(source)
				code := FrameError
				if capsule {
					reader = NewCapsuleReader(source)
					code = 0x10e
				}
				if _, err := reader.Next(); err != nil {
					t.Fatal(err)
				}
				data := make([]byte, 2)
				n, err := reader.Read(data)
				if partial && n != 1 || !partial && n != 2 {
					t.Fatal(n)
				}
				if terminal == interrupted {
					if err != interrupted {
						t.Fatal("lost source failure", err)
					}
				} else if terminal == io.EOF && !partial {
					if err != nil {
						t.Fatal("complete payload EOF", err)
					}
					continue
				} else {
					var protocol *Error
					if !errors.As(err, &protocol) || protocol.Code != code {
						t.Fatalf("capsule=%v partial=%v terminal=%v: %v", capsule, partial, terminal, err)
					}
				}
				if _, next := reader.Next(); next != err {
					t.Fatal("failure not sticky", next, err)
				}
			}
		}
	}
}

type terminalPayloadReader struct {
	data    []byte
	err     error
	partial bool
}

func (r *terminalPayloadReader) Read(p []byte) (int, error) {
	data := r.data
	if r.partial {
		data = data[:1]
	}
	return copy(p, data), r.err
}
