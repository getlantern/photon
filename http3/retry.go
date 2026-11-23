package http3

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"

	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/http3conn"
)

// The first attempt has closed its body before this function is called. Trailer
// values can depend on reading the original body, so GetBody alone cannot prove
// that an upload with trailers can be reproduced.
func replayRequest(request *http.Request) (*http.Request, error) {
	if request.Context().Err() != nil {
		return nil, context.Cause(request.Context())
	}
	if len(request.Trailer) != 0 {
		return nil, nil
	}
	copy := new(http.Request)
	*copy = *request
	if request.Body == nil || request.Body == http.NoBody {
		return copy, nil
	}
	if request.GetBody == nil {
		return nil, nil
	}
	body, err := request.GetBody()
	if request.Context().Err() != nil {
		err = context.Cause(request.Context())
	}
	if err != nil {
		if body != nil {
			body.Close()
		}
		return nil, err
	}
	if body == nil {
		return nil, fmt.Errorf("http3: GetBody returned nil body")
	}
	copy.Body = body
	return copy, nil
}

// Evidence is collected at the concrete stream boundary, never from a request
// body or informational-response callback's error.
type rejectionTrackingStream struct {
	http3conn.ClientExchangeStream
	rejected atomic.Bool
}

func (s *rejectionTrackingStream) Read(p []byte) (int, error) {
	n, err := s.ClientExchangeStream.Read(p)
	if peerRejected(err) {
		s.rejected.Store(true)
	}
	return n, err
}
func peerRejected(err error) bool {
	var reset *quic.StreamError
	return errors.As(err, &reset) && reset.Remote && reset.ErrorCode == 0x10b
}

// A forwarded Early-Data field requires returning 425 to the upstream caller.
// Do not consume the response when replay cannot be attempted: callers retain
// the body and can choose their own policy. Unknown/ordinary methods are not
// upgraded into automatic status-based retries.
func retryTooEarly(request *http.Request, response *http.Response) bool {
	if request == nil || response == nil || response.StatusCode != http.StatusTooEarly {
		return false
	}
	if request.Method != MethodGet0RTT && request.Method != MethodHead0RTT {
		return false
	}
	if len(request.Trailer) != 0 || (request.Body != nil && request.Body != http.NoBody && request.GetBody == nil) {
		return false
	}
	for name := range request.Header {
		if strings.EqualFold(name, "Early-Data") {
			return false
		}
	}
	return true
}
