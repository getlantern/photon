package http3conn

import (
	"context"
	"errors"
	"fmt"
	"net/http"
)

var ErrExtendedConnectNotNegotiated = errors.New("http3: peer did not enable Extended CONNECT")

type extendedConnectKey struct{}

// ExtendedConnectProtocol preserves :protocol separately from the HTTP version
// and ordinary fields. Cloning a request retains its context metadata.
func ExtendedConnectProtocol(r *http.Request) string {
	if r == nil {
		return ""
	}
	protocol, _ := r.Context().Value(extendedConnectKey{}).(string)
	return protocol
}
func ValidExtendedConnectProtocol(protocol string) bool { return validToken(protocol) }

// WithExtendedConnectProtocol prepares metadata; it does not grant permission to
// send. Session must first receive the server's ENABLE_CONNECT_PROTOCOL setting.
func WithExtendedConnectProtocol(r *http.Request, protocol string) (*http.Request, error) {
	if r == nil || r.Method != "CONNECT" || !validToken(protocol) {
		return nil, fmt.Errorf("http3: invalid Extended CONNECT method or protocol")
	}
	return r.WithContext(context.WithValue(r.Context(), extendedConnectKey{}, protocol)), nil
}
