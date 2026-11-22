package http3

import "errors"

// ErrNoCachedConn means a cached-only request had no reusable pool entry.
var ErrNoCachedConn = errors.New("http3: no cached connection was available")

// RoundTripOpt controls one request, including any automatic retry.
type RoundTripOpt struct {
	// OnlyCachedConn prohibits creating a new connection. A connection already
	// dialing or recovering from early rejection remains an eligible pool entry.
	OnlyCachedConn bool
}
