package http3

import (
	"bytes"
	"errors"
	quic "github.com/getlantern/photon"
	h3 "github.com/getlantern/photon/internal/http3wire"
	"io"
	"testing"
	"testing/iotest"
)

func TestCapsuleParserLifetime(t *testing.T) {
	var data bytes.Buffer
	for _, v := range []struct {
		typ  CapsuleType
		data string
	}{{0, "abc"}, {6, ""}, {1 << 40, "last"}} {
		if err := WriteCapsule(&data, v.typ, []byte(v.data)); err != nil {
			t.Fatal(err)
		}
	}
	p := NewCapsuleParser(iotest.OneByteReader(&data))
	typ, r, err := p.Next()
	if err != nil || typ != 0 || r.Remaining() != 3 {
		t.Fatal(typ, r.Remaining(), err)
	}
	if b, err := r.ReadByte(); err != nil || b != 'a' {
		t.Fatal(b, err)
	}
	if _, _, err := p.Next(); !errors.Is(err, ErrCapsulePayloadPending) {
		t.Fatal(err)
	}
	copyReader := r
	if err := copyReader.Discard(); err != nil || r.Remaining() != 0 {
		t.Fatal(err, r.Remaining())
	}
	typ, empty, err := p.Next()
	if err != nil || typ != 6 || empty.Remaining() != 0 {
		t.Fatal(typ, err)
	}
	if _, err := r.ReadByte(); !errors.Is(err, ErrCapsuleReaderExpired) {
		t.Fatal(err)
	}
	if r.Remaining() != 0 {
		t.Fatal("expired remaining")
	}
	if err := empty.Discard(); err != nil {
		t.Fatal(err)
	}
	typ, last, err := p.Next()
	if err != nil || typ != 1<<40 {
		t.Fatal(typ, err)
	}
	b, err := io.ReadAll(last)
	if err != nil || string(b) != "last" {
		t.Fatal(string(b), err)
	}
	if _, _, err := p.Next(); err != io.EOF {
		t.Fatal(err)
	}
	if _, _, err := p.Next(); err != io.EOF {
		t.Fatal(err)
	}
}
func TestCapsuleTruncation(t *testing.T) {
	// Nonminimal headers, unknown HTTP/2-reserved types and maximum varints.
	full := []byte{0x40, 6, 0x40, 3, 'a', 'b', 'c'}
	for cut := 1; cut < len(full); cut++ {
		p := NewCapsuleParser(bytes.NewReader(full[:cut]))
		_, r, err := p.Next()
		if err == nil {
			_, err = io.ReadAll(r)
		}
		var malformed *CapsuleError
		if !errors.As(err, &malformed) || malformed.Code() != 0x10e || !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("cut %d: %v", cut, err)
		}
		if _, _, next := p.Next(); next != err {
			t.Fatal("error not sticky", next, err)
		}
	}
	p := NewCapsuleParser(bytes.NewReader([]byte{0, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}))
	_, r, err := p.Next()
	if err != nil || r.Remaining() != 1<<62-1 {
		t.Fatal(r.Remaining(), err)
	}
	if err := r.Discard(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
}

type capsuleBadWriter struct {
	calls  int
	failAt int
	err    error
}

func (w *capsuleBadWriter) Write(b []byte) (int, error) {
	w.calls++
	if w.calls == w.failAt {
		return len(b) - 1, w.err
	}
	return len(b), nil
}
func TestCapsuleWriterFailures(t *testing.T) {
	sentinel := errors.New("sink failed")
	for _, stage := range []int{1, 2} {
		for _, err := range []error{nil, sentinel} {
			w := &capsuleBadWriter{failAt: stage, err: err}
			got := WriteCapsule(w, 0, []byte("abc"))
			want := err
			if want == nil {
				want = io.ErrShortWrite
			}
			if got != want || w.calls != stage {
				t.Fatal(got, w.calls)
			}
		}
	}
	var b bytes.Buffer
	if err := WriteCapsule(&b, 1<<62, nil); err == nil || b.Len() != 0 {
		t.Fatal(err, b.Len())
	}
	if err := WriteCapsule(&b, 0, nil); err != nil || !bytes.Equal(b.Bytes(), []byte{0, 0}) {
		t.Fatal(err, b.Bytes())
	}
}

type abortingCapsuleSource struct {
	io.Reader
	calls int
	code  quic.StreamErrorCode
}

func (r *abortingCapsuleSource) CloseWithError(code quic.StreamErrorCode, _ string) error {
	r.calls++
	r.code = code
	return nil
}
func TestCapsuleParserAutomaticAbort(t *testing.T) {
	for _, data := range [][]byte{{0}, {0, 3, 'x'}, nil, {0, 0}} {
		source := &abortingCapsuleSource{Reader: bytes.NewReader(data)}
		parser := NewCapsuleParser(source)
		_, payload, err := parser.Next()
		if err == nil {
			err = payload.Discard()
		}
		malformed := len(data) == 1 || len(data) == 3
		if malformed {
			var capsule *CapsuleError
			if !errors.As(err, &capsule) || source.calls != 1 || source.code != 0x10e {
				t.Fatal(err, source.calls, source.code)
			}
			if _, _, again := parser.Next(); again != err || source.calls != 1 {
				t.Fatal("repeated abort", again, source.calls)
			}
		} else if source.calls != 0 {
			t.Fatal("clean capsule caused abort")
		}
	}
	connection := &h3.Error{Code: 0x106, Reason: "underlying DATA frame truncated"}
	source := &abortingCapsuleSource{Reader: iotest.ErrReader(connection)}
	_, _, err := NewCapsuleParser(source).Next()
	if err != connection || source.calls != 0 {
		t.Fatal("connection error reclassified", err, source.calls)
	}
}

func TestCapsuleUnexpectedEOFAborts(t *testing.T) {
	source := &abortingCapsuleSource{Reader: io.MultiReader(bytes.NewReader([]byte{0, 3}), iotest.ErrReader(io.ErrUnexpectedEOF))}
	parser := NewCapsuleParser(source)
	_, payload, err := parser.Next()
	if err != nil {
		t.Fatal(err)
	}
	err = payload.Discard()
	var capsule *CapsuleError
	if !errors.As(err, &capsule) || source.calls != 1 || source.code != 0x10e {
		t.Fatal("direct unexpected EOF escaped request abort", err, source.calls, source.code)
	}
}

func FuzzPublicCapsuleFragmentation(f *testing.F) {
	f.Add([]byte{0, 0, 6, 3, 'a', 'b', 'c'})
	f.Add([]byte{0x40, 0, 0x40, 1, 7})
	f.Add([]byte{0, 3, 'x'})
	f.Fuzz(func(t *testing.T, data []byte) {
		sources := []*abortingCapsuleSource{
			{Reader: bytes.NewReader(data)},
			{Reader: iotest.OneByteReader(bytes.NewReader(data))},
			{Reader: iotest.DataErrReader(bytes.NewReader(data))},
		}
		var reference []byte
		var referenceError string
		for index, source := range sources {
			parser := NewCapsuleParser(source)
			var normalized bytes.Buffer
			var last error
			for count := 0; count <= len(data)/2+1; count++ {
				typ, payload, err := parser.Next()
				if err != nil {
					last = err
					break
				}
				value, err := io.ReadAll(payload)
				if err != nil {
					last = err
					break
				}
				if payload.Remaining() != 0 {
					t.Fatal("payload remaining after EOF")
				}
				if err := WriteCapsule(&normalized, typ, value); err != nil {
					t.Fatal(err)
				}
			}
			if last == nil {
				t.Fatal("parser did not terminate")
			}
			if _, _, again := parser.Next(); again != last {
				t.Fatal("non-sticky terminal state")
			}
			var malformed *CapsuleError
			expectedAborts := 0
			if errors.As(last, &malformed) {
				expectedAborts = 1
			}
			if source.calls != expectedAborts {
				t.Fatal("wrong abort count", source.calls, last)
			}
			if index == 0 {
				reference = append([]byte(nil), normalized.Bytes()...)
				referenceError = last.Error()
			} else if !bytes.Equal(reference, normalized.Bytes()) || referenceError != last.Error() {
				t.Fatal("fragmentation changed capsule results")
			}
		}
	})
}
