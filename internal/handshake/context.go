package handshake

// A pointer key keeps the TLS annotation separate from application context keys.
type quicVersionContextKey struct{}

var QUICVersionContextKey = &quicVersionContextKey{}
