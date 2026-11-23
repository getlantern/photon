package quic

import "encoding/hex"

// ConnectionID identifies a QUIC connection. Values are comparable and can be
// used as map keys. The zero value is invalid: it has no bytes, but differs from
// the zero-length ID that ConnectionIDFromBytes(nil) returns.
type ConnectionID struct {
	value string
	valid bool
}

// ConnectionIDFromBytes copies b into an immutable connection ID. It panics if
// b is longer than 20 bytes, the maximum supported by QUIC versions 1 and 2.
func ConnectionIDFromBytes(b []byte) ConnectionID {
	if len(b) > 20 {
		panic("quic: connection ID exceeds 20 bytes")
	}
	return ConnectionID{value: string(b), valid: true}
}

// Valid reports whether c is a connection ID, as opposed to the zero value.
func (c ConnectionID) Valid() bool { return c.valid }

// Len returns the number of bytes in the connection ID.
func (c ConnectionID) Len() int { return len(c.value) }

// Bytes returns an independent, mutable copy of the connection ID bytes.
func (c ConnectionID) Bytes() []byte { return []byte(c.value) }

// String returns lowercase hexadecimal, "(empty)" for a zero-length ID, or
// "(invalid)" for the zero value.
func (c ConnectionID) String() string {
	if !c.valid {
		return "(invalid)"
	}
	if c.value == "" {
		return "(empty)"
	}
	return hex.EncodeToString([]byte(c.value))
}

// ConnectionIDGenerator supplies unique, unlinkable local connection IDs.
// ConnectionIDLen must be constant and between 0 and 20. Zero-length IDs require
// address-based routing and cannot support migration or multiplexing connections
// to the same peer address. Generator methods must not reenter their Transport.
type ConnectionIDGenerator interface {
	GenerateConnectionID() (ConnectionID, error)
	ConnectionIDLen() int
}
