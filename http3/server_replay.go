package http3

import (
	"crypto/tls"
	"net/http"
)

// Local TLS completion cannot remove replay exposure inherited from another hop.
// The request adapter has already canonicalized every Early-Data instance to 1.
func replayHandler(next http.Handler, allow func(*http.Request) bool, handshake <-chan struct{}, state func() tls.ConnectionState) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		_, forwarded := request.Header["Early-Data"]
		complete := request.TLS != nil && request.TLS.HandshakeComplete
		if forwarded || !complete {
			accepted := allow != nil && allow(request)
			if !accepted {
				if forwarded {
					w.WriteHeader(http.StatusTooEarly)
					return
				}
				select {
				case <-handshake:
					current := state()
					request.TLS = &current
				case <-request.Context().Done():
					return
				}
			}
		}
		if request.Context().Err() != nil {
			return
		}
		next.ServeHTTP(w, request)
	})
}
