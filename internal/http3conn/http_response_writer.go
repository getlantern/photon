package http3conn

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/getlantern/photon/internal/qpack"
)

// HTTPResponseWriter adapts one response to Go handlers. Calls must be serialized.
// The owner calls Finish after the handler returns and handles failures through
// the stream/connection lifecycle. Final headers are snapshotted by WriteHeader
// and emitted on Write, Flush or Finish. DATA is unbuffered: automatic small-body
// Content-Length, request Expect handling, deadlines and panic recovery belong to
// the endpoint. Trailer permission is inherited from MessageWriter.
type HTTPResponseWriter struct {
	mapError       handlerErrorMap
	acceptTunnel   func() (*ServerTunnel, error)
	takeOver       func() (*RawStream, error)
	hijacked       bool // taken over; the response is the application's
	wrote          bool // the handler wrote body bytes
	writer         *MessageWriter
	header         http.Header
	snapshot       http.Header
	trailers       http.Header
	status         int
	sent, finished bool
	err            error
}

var _ http.ResponseWriter = (*HTTPResponseWriter)(nil)
var _ http.Flusher = (*HTTPResponseWriter)(nil)

func NewHTTPResponseWriter(writer *MessageWriter) (*HTTPResponseWriter, error) {
	if writer == nil || writer.message.config.Kind != ResponseFields || writer.message.body != nil || writer.closed || writer.err != nil {
		return nil, fmt.Errorf("http3: invalid HTTP response writer configuration")
	}
	return &HTTPResponseWriter{writer: writer, header: make(http.Header)}, nil
}
func (w *HTTPResponseWriter) Header() http.Header { return w.header }
func (w *HTTPResponseWriter) Err() error          { return w.mapError.apply(w.err) }
func (w *HTTPResponseWriter) WriteHeader(status int) {
	if w.hijacked {
		panic("http3: WriteHeader called after HTTPStream")
	}
	if w.err != nil || w.finished || w.status != 0 {
		return
	}
	if status < 100 || status > 599 {
		panic(fmt.Sprintf("http3: invalid WriteHeader code %d", status))
	}
	if status == 101 {
		w.err = &MessageError{"101 switching protocols prohibited in HTTP/3"}
		return
	}
	if status < 200 {
		w.err = w.writer.Headers(handlerFields(w.header, status, nil, false))
		return
	}
	w.status = status
	w.snapshot = w.header.Clone()
}
func (w *HTTPResponseWriter) commit(first []byte) error {
	if w.err != nil {
		return w.err
	}
	if w.sent {
		return nil
	}
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	w.trailers, w.err = announcedTrailers(w.snapshot.Values("Trailer"))
	if w.err != nil {
		return w.err
	}
	if _, exists := w.snapshot["Date"]; !exists {
		w.snapshot.Set("Date", time.Now().UTC().Format(http.TimeFormat))
	}
	if _, exists := w.snapshot["Content-Type"]; !exists && len(first) > 0 && w.status != 204 && w.status != 205 && w.status != 304 && w.snapshot.Get("Content-Encoding") == "" {
		w.snapshot.Set("Content-Type", http.DetectContentType(first))
	}
	w.err = w.writer.Headers(handlerFields(w.snapshot, w.status, w.trailers, false))
	if w.err == nil {
		w.sent = true
	}
	return w.err
}
func (w *HTTPResponseWriter) Write(p []byte) (n int, err error) {
	defer func() { err = w.mapError.apply(err) }()
	if w.hijacked {
		return 0, http.ErrHijacked
	}
	if w.finished {
		return 0, io.ErrClosedPipe
	}
	if err := w.commit(p); err != nil {
		return 0, err
	}
	if w.status == 204 || w.status == 205 || w.status == 304 {
		return 0, http.ErrBodyNotAllowed
	}
	w.wrote = w.wrote || len(p) > 0
	if w.writer.message.config.RequestMethod == "HEAD" {
		return len(p), nil
	}
	if len(p) == 0 {
		return 0, nil
	}
	n, err = w.writer.Write(p)
	if err != nil {
		w.err = err
	}
	return n, err
}
func (w *HTTPResponseWriter) Flush() { _ = w.FlushError() }
func (w *HTTPResponseWriter) FlushError() (err error) {
	defer func() { err = w.mapError.apply(err) }()
	if w.hijacked {
		return http.ErrHijacked
	}
	if w.finished {
		return io.ErrClosedPipe
	}
	return w.commit(nil)
}

// Finish emits populated declared trailers and TrailerPrefix fields, then FIN.
// It must not run concurrently with handler writes. Successful repeats are safe.
func (w *HTTPResponseWriter) Finish() error {
	if w.err != nil {
		return w.err
	}
	if w.finished {
		return nil
	}
	if err := w.commit(nil); err != nil {
		return err
	}
	fields := handlerFields(w.header, 0, w.trailers, true)
	if len(fields) > 0 {
		w.err = w.writer.Headers(fields)
	}
	if w.err == nil {
		w.err = w.writer.Close()
	}
	if w.err == nil {
		w.finished = true
	}
	return w.err
}
func handlerFields(header http.Header, status int, trailers http.Header, ending bool) []qpack.DecodedField {
	fields := make([]qpack.DecodedField, 0, len(header)+1)
	if status != 0 {
		fields = append(fields, qpack.DecodedField{Field: qpack.Field{Name: ":status", Value: strconv.Itoa(status)}})
	}
	keys := make([]string, 0, len(header))
	for key := range header {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		name := key
		prefixed := strings.HasPrefix(name, http.TrailerPrefix)
		if prefixed {
			name = strings.TrimPrefix(name, http.TrailerPrefix)
		}
		_, declared := trailers[http.CanonicalHeaderKey(name)]
		if ending != (prefixed || declared) {
			continue
		}
		for _, value := range header[key] {
			fields = append(fields, qpack.DecodedField{Field: qpack.Field{Name: strings.ToLower(name), Value: value}})
		}
	}
	return fields
}

// AcceptTunnel is installed by the request owner, which retains handler lifetime.
func (w *HTTPResponseWriter) AcceptTunnel() (tunnel *ServerTunnel, err error) {
	defer func() { err = w.mapError.apply(err) }()
	if w.acceptTunnel == nil {
		return nil, fmt.Errorf("http3: tunnel handoff unavailable")
	}
	return w.acceptTunnel()
}

// TakeOver is installed by the request owner. It moves the response side into
// a RawStream that outlives the handler, sending the response header first if
// WriteHeader set a final status. It fails once body bytes were written.
// Afterwards WriteHeader panics and Write returns http.ErrHijacked.
func (w *HTTPResponseWriter) TakeOver() (stream *RawStream, err error) {
	defer func() { err = w.mapError.apply(err) }()
	if w.takeOver == nil {
		return nil, fmt.Errorf("http3: stream takeover unavailable")
	}
	return w.takeOver()
}
