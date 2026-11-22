//go:build !linux && !darwin

package transport

// Platform-specific nonfragmentation configuration remains unimplemented here.
// Packet construction still uses the conservative 1200-byte maximum.
func configureUDPSocket(socketController) error { return nil }

const udpSupportsMTUDiscovery = false
