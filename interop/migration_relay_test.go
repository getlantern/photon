package interop

import (
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"testing"
)

// migrationRelay gives each client source tuple an independent NAT binding.
// Each migrated tuple loses at most its first packet in either selected direction.
type migrationRelay struct {
	versionNegotiations         atomic.Uint32
	clientFirstVersion          atomic.Uint32
	clientBits, serverBits      bitCounts
	front                       *net.UDPConn
	mu                          sync.Mutex
	backs                       map[string]*net.UDPConn
	largeResponseLoss           map[string]int
	largeResponseDrops          atomic.Int32
	done                        chan struct{}
	readers                     sync.WaitGroup
	requestDrops, responseDrops atomic.Int32
}

func newMigrationRelay(t *testing.T, remote *net.UDPAddr, dropRequest, dropResponse bool) *migrationRelay {
	t.Helper()
	return newMigrationRelayWithTrace(t, remote, dropRequest, dropResponse, nil)
}

func newMigrationRelayWithTrace(t *testing.T, remote *net.UDPAddr, dropRequest, dropResponse bool, trace *migrationTrace) *migrationRelay {
	t.Helper()
	network, loopback := "udp4", net.IPv4(127, 0, 0, 1)
	if remote.IP.To4() == nil {
		network, loopback = "udp6", net.IPv6loopback
	}
	front, err := net.ListenUDP(network, &net.UDPAddr{IP: loopback})
	if err != nil {
		t.Fatal(err)
	}
	r := &migrationRelay{front: front, backs: make(map[string]*net.UDPConn), largeResponseLoss: make(map[string]int), done: make(chan struct{})}
	go func() {
		defer close(r.done)
		buffer := make([]byte, 65535)
		for {
			n, client, err := front.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			if n >= 5 && buffer[0]&0x80 != 0 {
				r.clientFirstVersion.CompareAndSwap(0, binary.BigEndian.Uint32(buffer[1:5]))
			}
			recordMigrationBit(buffer[:n], &r.clientBits)
			key := client.String()
			r.mu.Lock()
			back := r.backs[key]
			migrated := len(r.backs) > 0
			fresh := back == nil
			if fresh {
				back, err = net.ListenUDP(network, &net.UDPAddr{IP: loopback})
				if err == nil {
					r.backs[key] = back
				}
			}
			r.mu.Unlock()
			if err != nil {
				return
			}
			if trace != nil {
				trace.packet("request", key, back.LocalAddr().String(), buffer[:n], fresh && migrated && dropRequest)
			}
			if fresh {
				r.readers.Add(1)
				go func(back *net.UDPConn, client *net.UDPAddr, drop bool) {
					defer r.readers.Done()
					peer := back.LocalAddr().String()
					buf := make([]byte, 65535)
					for {
						n, _, err := back.ReadFromUDP(buf)
						if err != nil {
							return
						}
						if n >= 5 && buf[0]&0x80 != 0 && binary.BigEndian.Uint32(buf[1:5]) == 0 {
							r.versionNegotiations.Add(1)
						}
						recordMigrationBit(buf[:n], &r.serverBits)
						if trace != nil {
							trace.packet("response", remote.String(), peer, buf[:n], drop)
						}
						if drop {
							drop = false
							r.responseDrops.Add(1)
							continue
						}
						// Optional bulk-transfer loss, armed only after path validation.
						if n >= 512 {
							r.mu.Lock()
							lose := r.largeResponseLoss[peer] > 0
							if lose {
								r.largeResponseLoss[peer]--
							}
							r.mu.Unlock()
							if lose {
								r.largeResponseDrops.Add(1)
								continue
							}
						}
						if _, err := front.WriteToUDP(buf[:n], client); err != nil {
							return
						}
					}
				}(back, client, migrated && dropResponse)
				if migrated && dropRequest {
					r.requestDrops.Add(1)
					continue
				}
			}
			if _, err := back.WriteToUDP(buffer[:n], remote); err != nil {
				return
			}
		}
	}()
	return r
}

func (r *migrationRelay) peer(client string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if socket := r.backs[client]; socket != nil {
		return socket.LocalAddr().String()
	}
	return ""
}

func (r *migrationRelay) close() {
	r.front.Close()
	<-r.done // Stop all additions before joining the dynamically created readers.
	for _, socket := range r.backs {
		socket.Close()
	}
	r.readers.Wait()
}

// dropLargeResponses arms a bounded burst for one server-facing NAT binding.
// The size threshold excludes small ACK/control packets; payload stays opaque.
func (r *migrationRelay) dropLargeResponses(peer string, count int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.largeResponseLoss[peer] = count
}

// Counts observable short-header bits without decrypting or rewriting traffic.
func recordMigrationBit(data []byte, counts *bitCounts) {
	if len(data) == 0 || data[0]&0x80 != 0 {
		return
	}
	if data[0]&0x40 == 0 {
		counts.clear.Add(1)
	} else {
		counts.set.Add(1)
	}
}
