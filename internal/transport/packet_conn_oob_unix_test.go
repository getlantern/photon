//go:build linux || darwin

package transport

import (
	"context"
	"net"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
)

type observedOOBSocket struct {
	*net.UDPConn
	reads, writes, controls atomic.Int32
	lastMark                atomic.Int32
}

func (c *observedOOBSocket) SyscallConn() (syscall.RawConn, error) {
	c.controls.Add(1)
	return c.UDPConn.SyscallConn()
}
func (c *observedOOBSocket) ReadMsgUDP(b, oob []byte) (n, oobn, flags int, addr *net.UDPAddr, err error) {
	n, oobn, flags, addr, err = c.UDPConn.ReadMsgUDP(b, oob)
	if err == nil {
		c.lastMark.Store(int32(receivedECN(oob[:oobn], flags)))
		c.reads.Add(1)
	}
	return
}
func (c *observedOOBSocket) WriteMsgUDP(b, oob []byte, addr *net.UDPAddr) (int, int, error) {
	c.writes.Add(1)
	return c.UDPConn.WriteMsgUDP(b, oob, addr)
}

var _ OOBCapablePacketConn = (*observedOOBSocket)(nil)

func TestOOBWrapperReceiveAndCapabilities(t *testing.T) {
	for _, address := range []string{"127.0.0.1:0", "[::1]:0"} {
		t.Run(address, func(t *testing.T) {
			local, err := net.ResolveUDPAddr("udp", address)
			if err != nil {
				t.Fatal(err)
			}
			socket, err := net.ListenUDP("udp", local)
			if err != nil {
				t.Fatal(err)
			}
			defer socket.Close()
			wrapper := &observedOOBSocket{UDPConn: socket}
			e, err := NewClientOnlyEndpoint(8, 2)
			if err != nil {
				t.Fatal(err)
			}
			runtime, err := NewPacketServerBorrowed(wrapper, e)
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close()
			if err := runtime.Do(context.Background(), func(e *ServerEndpoint) error {
				if !e.config.enableECN || !e.config.enablePathMTUDiscovery {
					t.Error("OOB capabilities lost")
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if wrapper.controls.Load() < 2 {
				t.Fatal("wrapper socket control bypassed")
			}
			assertUDPNoFragment(t, socket)
			sender, err := net.ListenUDP("udp", local)
			if err != nil {
				t.Fatal(err)
			}
			defer sender.Close()
			if _, err := writeECNDatagram(sender, []byte("unknown routed payload"), socket.LocalAddr().(*net.UDPAddr).AddrPort(), recovery.ECT1); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			for wrapper.reads.Load() == 0 {
				select {
				case <-tick.C:
				case <-ctx.Done():
					t.Fatal("wrapper ReadMsgUDP not called")
				}
			}
			if ECN(wrapper.lastMark.Load()) != ECN(recovery.ECT1) {
				t.Fatal("lost incoming ECN", wrapper.lastMark.Load())
			}
			runtime.Close()
			if _, err := socket.WriteToUDP([]byte("caller owns"), socket.LocalAddr().(*net.UDPAddr)); err != nil {
				t.Fatal(err)
			}
		})
	}
}
