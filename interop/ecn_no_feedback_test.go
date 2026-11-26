//go:build linux || darwin

package interop

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
	"golang.org/x/sys/unix"
)

// Expose only PacketConn to upstream, making ECN unavailable to its transport.
// Independently observe received kernel markings before withholding metadata.
// Packet payloads, addresses, and upstream's normal ACK generation are intact.
type ecnBlindConn struct {
	net.PacketConn
	socket *net.UDPConn
	mu     sync.Mutex
	marks  []uint8
}

func (c *ecnBlindConn) ReadFrom(b []byte) (int, net.Addr, error) {
	var control [128]byte
	n, on, flags, peer, err := c.socket.ReadMsgUDP(b, control[:])
	if err != nil {
		return n, peer, err
	}
	if flags&unix.MSG_CTRUNC != 0 {
		return 0, peer, fmt.Errorf("truncated ECN observation")
	}
	messages, err := unix.ParseSocketControlMessage(control[:on])
	if err != nil {
		return 0, peer, err
	}
	mark := uint8(255)
	tos := unix.IP_TOS
	if runtime.GOOS == "darwin" {
		tos = unix.IP_RECVTOS
	}
	for _, m := range messages {
		if m.Header.Level == unix.IPPROTO_IP && m.Header.Type == int32(tos) && len(m.Data) > 0 {
			mark = m.Data[0] & 3
		}
		if m.Header.Level == unix.IPPROTO_IPV6 && m.Header.Type == unix.IPV6_TCLASS && len(m.Data) == 4 {
			mark = uint8(binary.NativeEndian.Uint32(m.Data) & 3)
		}
	}
	if mark == 255 {
		return 0, peer, fmt.Errorf("missing ECN observation")
	}
	c.mu.Lock()
	if len(c.marks) < 4096 {
		c.marks = append(c.marks, mark)
	}
	c.mu.Unlock()
	return n, peer, nil
}

func TestECNSenderStopsWithoutPeerFeedback(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		for _, address := range []string{"127.0.0.1:0", "[::1]:0"} {
			for _, server := range []bool{false, true} {
				t.Run(fmt.Sprintf("%x/%s/server=%v", v, address, server), func(t *testing.T) {
					ct, st := tlsConfigs(t)
					addr, err := net.ResolveUDPAddr("udp", address)
					if err != nil {
						t.Fatal(err)
					}
					socket, err := net.ListenUDP("udp", addr)
					if err != nil {
						t.Fatal(err)
					}
					defer socket.Close()
					raw, err := socket.SyscallConn()
					if err != nil {
						t.Fatal(err)
					}
					var optionErr error
					err = raw.Control(func(fd uintptr) {
						level, option := unix.IPPROTO_IP, unix.IP_RECVTOS
						if addr.IP.To4() == nil {
							level, option = unix.IPPROTO_IPV6, unix.IPV6_RECVTCLASS
						}
						optionErr = unix.SetsockoptInt(int(fd), level, option, 1)
					})
					if err != nil || optionErr != nil {
						t.Fatal(err, optionErr)
					}
					observer := &ecnBlindConn{PacketConn: socket, socket: socket}
					transport := &upstream.Transport{Conn: observer}
					defer transport.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					config := &quic.Config{Versions: []quic.Version{v}, EnableDatagrams: true, CongestionControl: quic.CongestionControlNewReno}
					peerConfig := &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}, EnableDatagrams: true}
					var c *quic.Conn
					var peer *upstream.Conn
					if server {
						listener, e := quic.ListenAddr(address, st, config)
						if e != nil {
							t.Fatal(e)
						}
						defer listener.Close()
						peer, err = transport.Dial(ctx, listener.Addr(), ct, peerConfig)
						if err != nil {
							t.Fatal(err)
						}
						defer peer.CloseWithError(0, "")
						c, err = listener.Accept(ctx)
						if err != nil {
							t.Fatal(err)
						}
						defer c.CloseWithError(0, "")
					} else {
						listener, e := transport.Listen(st, peerConfig)
						if e != nil {
							t.Fatal(e)
						}
						defer listener.Close()
						c, err = quic.DialAddr(ctx, listener.Addr().String(), ct, config)
						if err != nil {
							t.Fatal(err)
						}
						defer c.CloseWithError(0, "")
						peer, err = listener.Accept(ctx)
						if err != nil {
							t.Fatal(err)
						}
						defer peer.CloseWithError(0, "")
					}
					for i := 0; i < 24; i++ {
						payload := fmt.Sprintf("no ECN feedback %d", i)
						if err := c.SendDatagram([]byte(payload)); err != nil {
							t.Fatal(err)
						}
						b, err := peer.ReceiveDatagram(ctx)
						if err != nil || string(b) != payload {
							t.Fatal(string(b), err)
						}
						if err := peer.SendDatagram(b); err != nil {
							t.Fatal(err)
						}
						b, err = c.ReceiveDatagram(ctx)
						if err != nil || string(b) != payload {
							t.Fatal(string(b), err)
						}
					}
					observer.mu.Lock()
					marks := append([]uint8(nil), observer.marks...)
					observer.mu.Unlock()
					marked := false
					for _, m := range marks {
						if m == 2 {
							marked = true
						}
					}
					if !marked || len(marks) < 24 {
						t.Fatal("test never observed initial ECT probes", marks)
					}
					for _, m := range marks[len(marks)-8:] {
						if m != 0 {
							t.Fatal("ECN did not remain disabled", marks)
						}
					}
				})
			}
		}
	}
}
