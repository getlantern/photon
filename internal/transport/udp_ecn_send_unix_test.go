//go:build linux || darwin

package transport

import (
	"net"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
)

func TestSocketSendECNPerDatagram(t *testing.T)     { testSocketSendECNPerDatagram(t, false) }
func TestOOBWrapperSendECNPerDatagram(t *testing.T) { testSocketSendECNPerDatagram(t, true) }

// Include receive/drain cost so each successful send reaches a live socket and
// the benchmark cannot fill the receive queue. No reader goroutine is needed.
func BenchmarkSocketSendECN(b *testing.B) {
	for _, address := range []string{"127.0.0.1:0", "[::1]:0"} {
		b.Run(address, func(b *testing.B) {
			addr, err := net.ResolveUDPAddr("udp", address)
			if err != nil {
				b.Fatal(err)
			}
			receiver, err := net.ListenUDP("udp", addr)
			if err != nil {
				b.Fatal(err)
			}
			defer receiver.Close()
			sender, err := net.ListenUDP("udp", addr)
			if err != nil {
				b.Fatal(err)
			}
			defer sender.Close()
			peer := receiver.LocalAddr().(*net.UDPAddr).AddrPort()
			data := []byte("marked")
			var received [64]byte
			if err := receiver.SetReadDeadline(time.Now().Add(time.Minute)); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if n, err := writeECNDatagram(sender, data, peer, recovery.ECT0); err != nil || n != len(data) {
					b.Fatal(n, err)
				}
				if n, _, err := receiver.ReadFromUDPAddrPort(received[:]); err != nil || n != len(data) {
					b.Fatal(n, err)
				}
			}
		})
	}
}

func testSocketSendECNPerDatagram(t *testing.T, wrapped bool) {
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
			peer := receiver.LocalAddr().(*net.UDPAddr).AddrPort()
			runtime := &UDPServer{socket: sender}
			var wrapper *observedOOBSocket
			if wrapped {
				wrapper = &observedOOBSocket{UDPConn: sender}
				runtime.packetConn = wrapper
			}
			// Switch repeatedly on one socket, including explicit zero, so an ECT
			// marking must not leak to the next connection's datagram.
			for _, mark := range []recovery.ECNMark{recovery.ECT0, recovery.NotECT, recovery.ECT1, recovery.NotECT, recovery.ECT0} {
				if n, err := runtime.writeOutbound(&Outbound{Data: []byte("marked"), Peer: peer, ECN: mark}); err != nil || n != 6 {
					t.Fatal(n, err)
				}
				if err := receiver.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				var data [100]byte
				var oob [128]byte
				n, on, flags, _, err := receiver.ReadMsgUDPAddrPort(data[:], oob[:])
				if err != nil {
					t.Fatal(err)
				}
				if string(data[:n]) != "marked" || receivedECN(oob[:on], flags) != ECN(mark) {
					t.Fatalf("mark=%d received=%d", mark, receivedECN(oob[:on], flags))
				}
			}
			if wrapped && wrapper.writes.Load() != 3 {
				t.Fatal("OOB wrapper bypassed", wrapper.writes.Load())
			}
			if n, err := writeECNDatagram(sender, []byte("bad"), peer, recovery.ECNMark(3)); n != 0 || err == nil {
				t.Fatal("originated CE", n, err)
			}
		})
	}
}
