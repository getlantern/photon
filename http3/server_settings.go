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
}

func (w *settingsResponseWriter) Unwrap() http.ResponseWriter { return w.HTTPResponseWriter }

var _ Settingser = (*settingsResponseWriter)(nil)
var _ http.Flusher = (*settingsResponseWriter)(nil)
