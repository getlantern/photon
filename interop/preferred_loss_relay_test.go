package interop

import (
	"errors"
	"net"
	"sync/atomic"
	"syscall"
	"testing"
)

// This relay impairs the original address only. The authenticated preferred
// address remains directly reachable, so migration bypasses the relay.
func preferredLossRelay(t *testing.T, network string, server *net.UDPAddr) (*net.UDPAddr, *atomic.Uint32) {
	t.Helper()
	socket, err := net.ListenUDP(network, &net.UDPAddr{IP: server.IP})
	if err != nil {
		t.Fatal(err)
	}
	drops := new(atomic.Uint32)
	done := make(chan struct{})
	go func() {
		defer close(done)
		var client *net.UDPAddr
		var droppedClient, droppedServer bool
		var buffer [65535]byte
		for {
			n, from, err := socket.ReadFromUDP(buffer[:])
			if errors.Is(err, syscall.ECONNREFUSED) {
				continue
			}
			if err != nil {
				return
			}
			target := server
			if from.AddrPort() == server.AddrPort() {
				if client == nil {
					continue
				}
				if !droppedServer {
					droppedServer = true
					drops.Add(1)
					continue
				}
				target = client
			} else {
				client = from
				if !droppedClient {
					droppedClient = true
					drops.Add(1)
					continue
				}
			}
			if _, err := socket.WriteToUDP(buffer[:n], target); err != nil && !errors.Is(err, syscall.ECONNREFUSED) {
				return
			}
		}
	}()
	t.Cleanup(func() { socket.Close(); <-done })
	return socket.LocalAddr().(*net.UDPAddr), drops
}
