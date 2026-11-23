package http3

import (
	"net/http"

	"github.com/getlantern/photon/internal/http3conn"
)

// Embed the concrete writer so Flush, FlushError and AcceptTunnel retain their
// existing behavior and error adaptation through the public handler boundary.
type settingsResponseWriter struct {
	*http3conn.HTTPResponseWriter
	settingsView
	stream *Stream
}

// HTTPStream implements HTTPStreamer.
func (w *settingsResponseWriter) HTTPStream() (*Stream, error) {
	if w.stream == nil {
		raw, err := w.HTTPResponseWriter.TakeOver()
		if err != nil {
			return nil, err
		}
		w.stream = &Stream{datagramStream: raw}
	}
	return w.stream, nil
}

func (w *settingsResponseWriter) Unwrap() http.ResponseWriter { return w.HTTPResponseWriter }

var _ Settingser = (*settingsResponseWriter)(nil)
var _ HTTPStreamer = (*settingsResponseWriter)(nil)
var _ http.Flusher = (*settingsResponseWriter)(nil)
