package http3

import (
	"compress/gzip"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
)

func requestsGzip(request *http.Request) bool {
	if request.Method == "HEAD" || request.Method == MethodHead0RTT || request.Method == "CONNECT" {
		return false
	}
	for name := range request.Header {
		if strings.EqualFold(name, "Accept-Encoding") || strings.EqualFold(name, "Range") {
			return false
		}
	}
	return true
}

func decodeGzipResponse(response *http.Response) {
	if response.StatusCode == 204 || response.StatusCode == 205 || response.StatusCode == 304 {
		return
	}
	encodings := response.Header.Values("Content-Encoding")
	if len(encodings) != 1 || !strings.EqualFold(strings.TrimSpace(encodings[0]), "gzip") {
		return
	}
	response.Header.Del("Content-Encoding")
	response.Header.Del("Content-Length")
	response.ContentLength = -1
	response.Uncompressed = true
	response.Body = &gzipBody{settingsView: settingsFrom(response.Body), source: response.Body}
}

// Initialize lazily: RoundTrip returns final headers without waiting for payload.
// Close interrupts the source before waiting for an in-progress Read to return.
type gzipBody struct {
	settingsView
	source    io.ReadCloser
	mu        sync.Mutex
	reader    *gzip.Reader
	readErr   error
	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

func (b *gzipBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	if len(p) == 0 {
		return 0, nil
	}
	if b.readErr != nil {
		return 0, b.readErr
	}
	if b.reader == nil {
		b.reader, b.readErr = gzip.NewReader(b.source)
		if b.readErr != nil {
			return 0, b.readErr
		}
	}
	n, err := b.reader.Read(p)
	if err != nil {
		b.readErr = err
	}
	return n, err
}
func (b *gzipBody) Close() error {
	b.closeOnce.Do(func() {
		b.closed.Store(true)
		b.closeErr = b.source.Close()
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.reader != nil {
			if err := b.reader.Close(); b.closeErr == nil {
				b.closeErr = err
			}
		}
	})
	return b.closeErr
}
