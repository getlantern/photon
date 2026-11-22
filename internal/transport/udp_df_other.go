//go:build !linux && !darwin

package transport

// Platform-specific nonfragmentation configuration remains unimplemented here.
// Upward MTU discovery is disabled; the configured initial packet size still applies.
func configureUDPSocket(socketController) error { return nil }

const udpSupportsMTUDiscovery = false
