//go:build linux || darwin

package transport

import (
	"bytes"
	"context"
	"fmt"
	"github.com/getlantern/photon/internal/wire"
	"net"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestSocketReceiveECN(t *testing.T) {
	for _, address := range []string{"127.0.0.1:0", "[::1]:0"} {
		t.Run(address, func(t *testing.T) {
			addr, err := net.ResolveUDPAddr("udp", address)
			if err != nil {
				t.Fatal(err)
			}
			receiver, err := net.ListenUDP("udp", addr)
			if err != nil {
				t.Fatal(err)
			}
			defer receiver.Close()
			configureReceiveECN(receiver)
			sender, err := net.ListenUDP("udp", addr)
			if err != nil {
				t.Fatal(err)
			}
			defer sender.Close()
			raw, err := sender.SyscallConn()
			if err != nil {
				t.Fatal(err)
			}
			var messages [][]byte
			for _, mark := range []ECN{ECNNotECT, ECNECT0, ECNECT1, ECNCE} {
				var optionErr error
				err = raw.Control(func(fd uintptr) {
					level, option := unix.IPPROTO_IP, unix.IP_TOS
					if addr.IP.To4() == nil {
						level, option = unix.IPPROTO_IPV6, unix.IPV6_TCLASS
					}
					optionErr = unix.SetsockoptInt(int(fd), level, option, 0x28|int(mark))
				})
				if err != nil || optionErr != nil {
					t.Fatal(err, optionErr)
				}
				if _, err := sender.WriteToUDP([]byte("ECN datagram"), receiver.LocalAddr().(*net.UDPAddr)); err != nil {
					t.Fatal(err)
				}
				receiver.SetReadDeadline(time.Now().Add(time.Second))
				var payload [100]byte
				var oob [128]byte
				n, oobn, flags, _, err := receiver.ReadMsgUDPAddrPort(payload[:], oob[:])
				if err != nil || string(payload[:n]) != "ECN datagram" {
					t.Fatal(err)
				}
				if got := receivedECN(oob[:oobn], flags); got != mark {
					t.Fatalf("got marking %d, want %d; metadata %x", got, mark, oob[:oobn])
				}
				messages = append(messages, bytes.Clone(oob[:oobn]))
				if receivedECN(oob[:oobn], flags|unix.MSG_CTRUNC) != ECNUnknown {
					t.Fatal("truncated metadata trusted")
				}
			}
			if receivedECN(append(messages[0], messages[1]...), 0) != ECNUnknown {
				t.Fatal("conflicting metadata trusted")
			}
			if receivedECN(nil, 0) != ECNUnknown || receivedECN([]byte{1}, 0) != ECNUnknown {
				t.Fatal("missing or malformed metadata trusted")
			}
			if !udpPayloadTruncated(unix.MSG_TRUNC) || udpPayloadTruncated(0) {
				t.Fatal("payload truncation flag")
			}
		})
	}
}

func TestUDPRuntimeECN(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, address := range []string{"127.0.0.1:0", "[::1]:0"} {
			t.Run(fmt.Sprintf("%x/%s", v, address), func(t *testing.T) {
				c, peer := testPair(t, v)
				drivePair(t, c, peer, false, false)
				addr, _ := net.ResolveUDPAddr("udp", address)
				sender, err := net.ListenUDP("udp", addr)
				if err != nil {
					t.Fatal(err)
				}
				defer sender.Close()
				receiver, err := net.ListenUDP("udp", addr)
				if err != nil {
					t.Fatal(err)
				}
				configureReceiveECN(sender)
				endpoint, err := NewClientEndpoint(c, sender.LocalAddr().(*net.UDPAddr).AddrPort())
				if err != nil {
					receiver.Close()
					t.Fatal(err)
				}
				runtime, err := NewUDPServer(receiver, endpoint)
				if err != nil {
					receiver.Close()
					t.Fatal(err)
				}
				defer runtime.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				raw, err := sender.SyscallConn()
				if err != nil {
					t.Fatal(err)
				}
				var optionErr error
				err = raw.Control(func(fd uintptr) {
					level, option := unix.IPPROTO_IP, unix.IP_TOS
					if addr.IP.To4() == nil {
						level, option = unix.IPPROTO_IPV6, unix.IPV6_TCLASS
					}
					optionErr = unix.SetsockoptInt(int(fd), level, option, int(ECNCE))
				})
				if err != nil || optionErr != nil {
					t.Fatal(err, optionErr)
				}
				packet := makePeerPacket(t, peer, applicationSpace, 100, wire.PingFrame{})
				if _, err := sender.WriteToUDP(packet, receiver.LocalAddr().(*net.UDPAddr)); err != nil {
					t.Fatal(err)
				}
				err = runtime.Wait(ctx, func(*ServerEndpoint) (bool, error) {
					tracker := c.spaces[applicationSpace].received
					if tracker == nil || !tracker.haveECN {
						return false, nil
					}
					if tracker.ecn != [3]uint64{0, 0, 1} {
						return false, fmt.Errorf("wrong runtime ECN counts: %v", tracker.ecn)
					}
					return true, nil
				})
				if err != nil {
					t.Fatal(err)
				}
				// marked requires the ACK's datagram to carry ECT(0): true while the
				// path is still marking its ECN test flight.
				readACK := func(number uint64, want [3]uint64, marked bool) {
					t.Helper()
					deadline, _ := ctx.Deadline()
					if err := sender.SetReadDeadline(deadline); err != nil {
						t.Fatal(err)
					}
					var buffer [65535]byte
					for {
						var oob [128]byte
						n, on, flags, _, err := sender.ReadMsgUDPAddrPort(buffer[:], oob[:])
						if err != nil {
							t.Fatal("wire ACK", err)
						}
						if mark := receivedECN(oob[:on], flags); marked && mark != ECNECT0 {
							t.Fatalf("runtime did not mark ECN probe: %v", mark)
						}
						h, err := wire.ParseHeader(buffer[:n], len(peer.config.LocalCID))
						if err != nil {
							t.Fatal(err)
						}
						_, plain, _, err := peer.readKeys(applicationSpace).Open(buffer[:n], h.PNOffset, peer.spaces[applicationSpace].received.Expected())
						if err != nil {
							t.Fatal("ACK authentication", err)
						}
						frames, _, err := parsePacketFrames(plain, wire.FrameContext{PacketType: wire.OneRTT, SenderIsServer: false})
						if err != nil {
							t.Fatal(err)
						}
						for _, frame := range frames {
							ack, ok := frame.(wire.AckFrame)
							if !ok {
								continue
							}
							covered := false
							for _, span := range ack.Ranges {
								covered = covered || (span.Smallest <= number && number <= span.Largest)
							}
							if !covered {
								continue
							}
							if ack.ECN == nil || *ack.ECN != want {
								t.Fatalf("wire ECN counts %v, want %v", ack.ECN, want)
							}
							return
						}
					}
				}
				readACK(100, [3]uint64{0, 0, 1}, true)
				// Retransmitting the same protected packet must not inflate CE.
				if _, err := sender.WriteToUDP(packet, receiver.LocalAddr().(*net.UDPAddr)); err != nil {
					t.Fatal(err)
				}
				readACK(100, [3]uint64{0, 0, 1}, true)
				err = raw.Control(func(fd uintptr) {
					level, option := unix.IPPROTO_IP, unix.IP_TOS
					if addr.IP.To4() == nil {
						level, option = unix.IPPROTO_IPV6, unix.IPV6_TCLASS
					}
					optionErr = unix.SetsockoptInt(int(fd), level, option, int(ECNECT0))
				})
				if err != nil || optionErr != nil {
					t.Fatal(err, optionErr)
				}
				// Out of order (101 skipped), so it is acknowledged at once, while
				// the path is still marking its ECN test flight.
				fresh := makePeerPacket(t, peer, applicationSpace, 102, wire.PingFrame{})
				if _, err := sender.WriteToUDP(fresh, receiver.LocalAddr().(*net.UDPAddr)); err != nil {
					t.Fatal(err)
				}
				readACK(102, [3]uint64{1, 0, 1}, true)
				// An in-order packet's ACK is delayed and sent by the runtime's
				// timer. It still reports the counts, but may leave after the ECN
				// test period ends, unmarked.
				inOrder := makePeerPacket(t, peer, applicationSpace, 103, wire.PingFrame{})
				if _, err := sender.WriteToUDP(inOrder, receiver.LocalAddr().(*net.UDPAddr)); err != nil {
					t.Fatal(err)
				}
				readACK(103, [3]uint64{2, 0, 1}, false)
			})
		}
	}
}
