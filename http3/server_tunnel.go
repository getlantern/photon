package http3

import (
	"fmt"
	"net/http"

	"github.com/getlantern/photon/internal/http3conn"
)

// ServerTunnel carries unframed CONNECT bytes in HTTP/3 DATA frames. The handler
// retains ownership and must join its I/O workers before returning. Read and Write
// may run concurrently. CloseWrite ends sending while reads remain possible;
// Close interrupts both directions. Handler return finishes sending and closes
// receiving. A tunnel cannot outlive its handler; HTTPStreamer provides a stream
// that can.
type ServerTunnel = http3conn.ServerTunnel

// AcceptTunnel sends a successful CONNECT response and transfers DATA
// I/O from ResponseWriter/request.Body to the returned tunnel. Call it before
// reading the body or committing final headers. Set response headers and an
// optional 2xx status first; the default is 200. Length and trailers are forbidden.
// After acceptance, use only the tunnel for I/O. Wrappers must implement Unwrap.
// Extended CONNECT handlers must validate their extension handshake first.
// HTTP datagrams require additional extension negotiation.
func AcceptTunnel(w http.ResponseWriter) (*ServerTunnel, error) {
	for w != nil {
		if accepter, ok := w.(interface {
			AcceptTunnel() (*http3conn.ServerTunnel, error)
		}); ok {
			return accepter.AcceptTunnel()
		}
		wrapper, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			break
		}
		w = wrapper.Unwrap()
	}
	return nil, fmt.Errorf("http3: response writer does not support tunnels")
}
