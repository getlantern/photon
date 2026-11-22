package quic

// Clone returns a shallow copy of c. Scalar field changes on the copy do not
// change c. Slices, pointers, callbacks, and TokenStore remain shared; copy their
// referenced data separately before modifying it independently. The caller must
// not modify c concurrently with Clone. The receiver must be non-nil.
func (c *Config) Clone() *Config {
	cloned := *c
	return &cloned
}
