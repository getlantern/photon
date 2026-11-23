package interop

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

// Hold short-header server packets, including HANDSHAKE_DONE, while allowing
// the cryptographic handshake to finish. The wrapper hides batching/OOB methods
// so every outgoing datagram passes through WriteTo.
type confirmationHoldSocket struct {
	net.PacketConn
	mu       sync.Mutex
	released bool
	packets  [][]byte
	peers    []net.Addr
}

func (s *confirmationHoldSocket) WriteTo(p []byte, peer net.Addr) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.released && len(p) > 0 && p[0]&0x80 == 0 {
		s.packets = append(s.packets, append([]byte(nil), p...))
		s.peers = append(s.peers, peer)
		return len(p), nil
	}
	return s.PacketConn.WriteTo(p, peer)
}

func (s *confirmationHoldSocket) release() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.released = true
	count := len(s.packets)
	for i, p := range s.packets {
		if _, err := s.PacketConn.WriteTo(p, s.peers[i]); err != nil {
			return count, err
		}
	}
	s.packets, s.peers = nil, nil
	return count, nil
}

func TestHandshakeTimeoutEndsBeforeIndependentConfirmation(t *testing.T) {
	for _, version := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			ct, st := tlsConfigs(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			udp, err := net.ListenPacket("udp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer udp.Close()
			socket := &confirmationHoldSocket{PacketConn: udp}
			peerTransport := &upstream.Transport{Conn: socket}
			defer peerTransport.Close()
			listener, err := peerTransport.Listen(st, &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			released := make(chan error, 1)
			go func() {
				peer, err := listener.Accept(ctx)
				if err != nil {
					released <- err
					return
				}
				// Accept proves the server verified the client's Finished. The client's
				// TLS completion necessarily preceded this point. Delay from here to
				// ensure release occurs beyond its absolute 500 ms handshake limit.
				timer := time.NewTimer(750 * time.Millisecond)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-ctx.Done():
					released <- ctx.Err()
					return
				case <-peer.Context().Done():
					released <- context.Cause(peer.Context())
					return
				}
				count, err := socket.release()
				if err == nil && count == 0 {
					err = fmt.Errorf("no confirmation datagrams held")
				}
				released <- err
			}()
			client, dialErr := quic.DialAddr(ctx, udp.LocalAddr().String(), ct, &quic.Config{
				Versions: []quic.Version{version}, HandshakeIdleTimeout: time.Second,
				HandshakeTimeout: 500 * time.Millisecond, MaxIdleTimeout: 3 * time.Second,
			})
			if client != nil {
				defer client.CloseWithError(0, "")
			}
			if dialErr != nil {
				cancel()
			}
			releaseErr := <-released
			if dialErr != nil || releaseErr != nil {
				t.Fatalf("dial=%v release=%v", dialErr, releaseErr)
			}
			if client == nil {
				t.Fatal("no confirmed connection")
			}
			if err := context.Cause(client.Context()); err != nil {
				t.Fatal("confirmed connection failed", err)
			}
		})
	}
}
