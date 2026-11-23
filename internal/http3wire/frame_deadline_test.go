package http3wire

import (
	"bytes"
	"io"
	"os"
	"testing"
)

// deadlineReader returns one byte per call, with an expired deadline before
// every byte: each call under test is interrupted wherever it can be.
type deadlineReader struct {
	data    []byte
	expired bool
}

func (r *deadlineReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	if r.expired = !r.expired; r.expired {
		return 0, os.ErrDeadlineExceeded
	}
	p[0], r.data = r.data[0], r.data[1:]
	return 1, nil
}

func TestReaderResumesAfterDeadline(t *testing.T) {
	// A four-byte type and a two-byte length give the header interior boundaries.
	// The deadline must be survivable at each.
	wire, err := AppendHeader(nil, Header{Type: 0x21 + 31*1000, Length: 3})
	if err != nil {
		t.Fatal(err)
	}
	wire = append(wire[:len(wire)-1], 0x40, 0x03) // the length again, not minimal
	wire = append(wire, "abc"...)
	wire, _ = AppendHeader(wire, Header{Type: Data, Length: 0})
	reader := NewReader(&deadlineReader{data: wire})
	next := func() Header {
		t.Helper()
		for expired := 0; ; expired++ {
			header, err := reader.Next()
			if err == nil {
				if expired == 0 {
					t.Fatal("header was never interrupted")
				}
				return header
			}
			if !IsDeadline(err) {
				t.Fatal(err)
			}
		}
	}
	if header := next(); header != (Header{Type: 0x21 + 31*1000, Length: 3}) {
		t.Fatal(header)
	}
	var payload bytes.Buffer
	for payload.Len() < 3 {
		buffer := make([]byte, 8)
		n, err := reader.Read(buffer)
		if err != nil && !IsDeadline(err) {
			t.Fatal(err)
		}
		payload.Write(buffer[:n])
	}
	if payload.String() != "abc" {
		t.Fatal(payload.String())
	}
	if header := next(); header != (Header{Type: Data}) {
		t.Fatal(header)
	}
	// Other errors stay sticky, including the stream's end.
	for i := 0; i < 2; i++ {
		_, err := reader.Next()
		for IsDeadline(err) {
			_, err = reader.Next()
		}
		if err != io.EOF {
			t.Fatal(err)
		}
	}
}

func TestReaderDeadlineInsideHeaderThenTruncation(t *testing.T) {
	reader := NewReader(&deadlineReader{data: []byte{0x40}})
	_, err := reader.Next()
	for IsDeadline(err) {
		_, err = reader.Next()
	}
	var failure *Error
	if e, ok := err.(*Error); ok {
		failure = e
	}
	if failure == nil || failure.Code != FrameError {
		t.Fatal("a header cut off after a deadline is not a clean end:", err)
	}
}
