package http3

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"
)

func gzipBytes(t *testing.T, payload string) []byte {
	t.Helper()
	var wire bytes.Buffer
	writer := gzip.NewWriter(&wire)
	if _, err := io.WriteString(writer, payload); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return wire.Bytes()
}
func TestGzipBodyValidationAndOwnership(t *testing.T) {
	valid := gzipBytes(t, "decoded")
	corrupt := bytes.Clone(valid)
	corrupt[len(corrupt)-8] ^= 0xff
	for _, tc := range []struct {
		name    string
		wire    []byte
		wantErr error
	}{
		{"valid", valid, nil}, {"header", []byte("invalid gzip header"), gzip.ErrHeader}, {"truncated", valid[:len(valid)-4], io.ErrUnexpectedEOF}, {"checksum", corrupt, gzip.ErrChecksum},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &poolTestBody{Reader: bytes.NewReader(tc.wire)}
			body := &gzipBody{source: source}
			payload, err := io.ReadAll(body)
			if !errors.Is(err, tc.wantErr) {
				t.Fatal(string(payload), err)
			}
			if tc.wantErr == nil && string(payload) != "decoded" {
				t.Fatal(string(payload))
			}
			if source.closed.Load() != 0 {
				t.Fatal("EOF or failure released ownership before Close")
			}
			if err := body.Close(); err != nil {
				t.Fatal(err)
			}
			if err := body.Close(); err != nil || source.closed.Load() != 1 {
				t.Fatal(err, source.closed.Load())
			}
			if _, err := body.Read(make([]byte, 1)); !errors.Is(err, io.ErrClosedPipe) {
				t.Fatal(err)
			}
		})
	}
}
func TestGzipCloseInterruptsInitialization(t *testing.T) {
	reader, writer := io.Pipe()
	defer writer.Close()
	source := &gzipBlockingSource{PipeReader: reader, entered: make(chan struct{})}
	body := &gzipBody{source: source}
	done := make(chan error, 1)
	go func() { _, err := body.Read(make([]byte, 1)); done <- err }()
	select {
	case <-source.entered:
	case <-time.After(time.Second):
		t.Fatal("read did not start")
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("read succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not interrupt read")
	}
}
func TestGzipNegotiation(t *testing.T) {
	for _, tc := range []struct {
		method string
		header http.Header
		want   bool
	}{
		{"GET", nil, true}, {"", nil, true}, {"POST", nil, true}, {"HEAD", nil, false}, {"CONNECT", nil, false},
		{"GET", http.Header{"accept-encoding": {"br"}}, false}, {"GET", http.Header{"Accept-Encoding": nil}, false}, {"GET", http.Header{"range": {"bytes=0-1"}}, false},
	} {
		request := &http.Request{Method: tc.method, Header: tc.header}
		if got := requestsGzip(request); got != tc.want {
			t.Fatal(tc, got)
		}
	}
	for _, tc := range []struct {
		status   int
		encoding []string
		want     bool
	}{
		{200, []string{"gzip"}, true}, {200, []string{"GZip"}, true}, {200, []string{"gzip, br"}, false}, {200, []string{"gzip", "gzip"}, false}, {204, []string{"gzip"}, false}, {205, []string{"gzip"}, false}, {304, []string{"gzip"}, false},
	} {
		response := &http.Response{StatusCode: tc.status, Header: http.Header{"Content-Encoding": tc.encoding, "Content-Length": {"42"}}, ContentLength: 42, Body: io.NopCloser(bytes.NewReader(nil))}
		decodeGzipResponse(response)
		if response.Uncompressed != tc.want {
			t.Fatal(tc)
		}
		if tc.want && (response.ContentLength != -1 || response.Header.Get("Content-Length") != "" || response.Header.Get("Content-Encoding") != "") {
			t.Fatal(response)
		}
		response.Body.Close()
	}
}

type gzipBlockingSource struct {
	*io.PipeReader
	entered chan struct{}
	once    sync.Once
}

func (s *gzipBlockingSource) Read(p []byte) (int, error) {
	s.once.Do(func() { close(s.entered) })
	return s.PipeReader.Read(p)
}
