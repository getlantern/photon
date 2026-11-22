package interop

import (
	"net/http"

	peerh3 "github.com/quic-go/quic-go/http3"
)

// peerHTTPStream takes over an upstream handler's stream. Upstream refuses once
// response body bytes were written; panicking aborts that handler visibly.
func peerHTTPStream(w http.ResponseWriter) *peerh3.Stream {
	stream, err := w.(peerh3.HTTPStreamer).HTTPStream()
	if err != nil {
		panic(err)
	}
	return stream
}
