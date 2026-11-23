package transport

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func foreignResult(t *testing.T, result <-chan foreignWriteResult) foreignWriteResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-time.After(time.Second):
		t.Fatal("missing write result")
		return foreignWriteResult{}
	}
}
func TestForwardWriteQueueOwnership(t *testing.T) {
	r, _ := NewRouter(8, 4)
	route := &handoffRoute{router: r}
	w := newPacketWriter(r)
	peer := netip.MustParseAddrPort("127.0.0.1:443")
	payload := []byte("original")
	notify := make(chan struct{}, 1)
	var results []<-chan foreignWriteResult
	for range 64 {
		result, err := w.offer(route, payload, peer, recovery.NotECT, notify)
		if err != nil {
			t.Fatal(err)
		}
		results = append(results, result)
	}
	payload[0] = 'X'
	if _, err := w.offer(route, payload, peer, recovery.NotECT, nil); !errors.Is(err, errOperationWouldBlock) {
		t.Fatal("queue capacity", err)
	}
	request := <-w.writes
	if string(request.data) != "original" || request.peer != peer {
		t.Fatal("request aliases caller bytes")
	}
	request.complete(len(request.data), nil)
	if got := foreignResult(t, results[0]); got.n != 8 || got.err != nil {
		t.Fatal(got)
	}
	w.close()
	w.close()
	for _, result := range results[1:] {
		if got := foreignResult(t, result); got.n != 0 || !errors.Is(got.err, ErrEndpointClosed) {
			t.Fatal(got)
		}
		select {
		case <-result:
			t.Fatal("duplicate completion")
		default:
		}
	}
	if _, err := w.offer(route, payload, peer, recovery.NotECT, nil); !errors.Is(err, ErrEndpointClosed) {
		t.Fatal("closed writer", err)
	}
}
func TestForwardWriteConcurrentClose(t *testing.T) {
	r, _ := NewRouter(8, 4)
	route := &handoffRoute{router: r}
	w := newPacketWriter(r)
	accepted := make(chan (<-chan foreignWriteResult), 800)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				result, err := w.offer(route, []byte{1}, netip.MustParseAddrPort("127.0.0.1:443"), recovery.NotECT, nil)
				if err == nil {
					accepted <- result
				} else if !errors.Is(err, ErrEndpointClosed) && !errors.Is(err, errOperationWouldBlock) {
					t.Error(err)
				}
			}
		})
	}
	w.close()
	wg.Wait()
	close(accepted)
	for result := range accepted {
		if got := foreignResult(t, result); !errors.Is(got.err, ErrEndpointClosed) {
			t.Fatal(got)
		}
	}
}

type foreignPlainSocket struct {
	net.PacketConn
	calls int
	n     int
	err   error
}

func (s *foreignPlainSocket) WriteTo(data []byte, peer net.Addr) (int, error) {
	s.calls++
	return s.n, s.err
}

type foreignControlSocket struct {
	OOBCapablePacketConn
	calls int
	n     int
	err   error
}

func (s *foreignControlSocket) WriteMsgUDP(data, oob []byte, peer *net.UDPAddr) (int, int, error) {
	s.calls++
	return s.n, len(oob), s.err
}
func TestForwardWriteDoesNotRetry(t *testing.T) {
	peer := netip.MustParseAddrPort("127.0.0.1:443")
	plain := new(foreignPlainSocket)
	if n, err := writeForeignDatagram(plain, []byte{1}, peer, recovery.ECT0); n != 0 || !errors.Is(err, errForeignECNUnsupported) || plain.calls != 0 {
		t.Fatal("generic marking wrote bytes", n, err)
	}
	for _, marking := range []recovery.ECNMark{recovery.NotECT, recovery.ECT0} {
		if marking != recovery.NotECT && !udpSupportsECN {
			continue
		}
		for _, n := range []int{0, 1} {
			for _, failure := range []error{nil, syscall.EINVAL, syscall.EMSGSIZE} {
				plain := &foreignPlainSocket{n: n, err: failure}
				control := &foreignControlSocket{n: n, err: failure}
				var got int
				var err error
				if marking == recovery.NotECT {
					got, err = writeForeignDatagram(plain, []byte{1}, peer, marking)
					if plain.calls != 1 {
						t.Fatal("repeated write")
					}
				} else {
					got, err = writeForeignDatagram(control, []byte{1}, peer, marking)
					if control.calls != 1 {
						t.Fatal("repeated control write")
					}
				}
				if got != n || err != failure {
					t.Fatal("lost exact result", got, err)
				}
			}
		}
	}
}
func TestForwardWriteRuntime(t *testing.T) {
	source, destination := forwardingRuntime(t), forwardingRuntime(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	h, err := destination.handoffTarget(ctx)
	if err != nil {
		t.Fatal(err)
	}
	route, err := source.registerHandoffRoute(ctx, h, nil)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := source.foreignWriteTarget(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sink, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	peer := sink.LocalAddr().(*net.UDPAddr).AddrPort()
	result, err := writer.offer(route, []byte("denied"), peer, recovery.NotECT, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := foreignResult(t, result); got.n != 0 || !errors.Is(got.err, errPathBinding) {
		t.Fatal("unbound write accepted", got)
	}
	c, _ := testPair(t, wire.Version1)
	if err := destination.Do(ctx, func(e *ServerEndpoint) error { return e.router.Add(c, peer) }); err != nil {
		t.Fatal(err)
	}
	if err := destination.bindHandoffConnection(ctx, route, c); err != nil {
		t.Fatal(err)
	}
	result, err = writer.offer(route, []byte("foreign output"), peer, recovery.NotECT, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := foreignResult(t, result); got.n != 14 || got.err != nil {
		t.Fatal(got)
	}
	var data [100]byte
	sink.SetReadDeadline(time.Now().Add(time.Second))
	n, from, err := sink.ReadFromUDP(data[:])
	if err != nil || string(data[:n]) != "foreign output" || from.String() != source.Addr().String() {
		t.Fatal("wrong socket/payload", n, from, err)
	}
	source.Close()
	if _, err := writer.offer(route, []byte{1}, peer, recovery.NotECT, nil); !errors.Is(err, ErrEndpointClosed) {
		t.Fatal("closed runtime accepted write", err)
	}
}

func TestForwardWriteRevokedBeforeExecution(t *testing.T) {
	r, _ := NewRouter(8, 4)
	h := newPacketHandoff()
	defer h.close()
	route := updateTestRoute(t, r, h, [][]byte{{1}})
	route.local = netip.MustParseAddrPort("127.0.0.1:443")
	if err := h.bindConnection(route, &Conn{}); err != nil {
		t.Fatal(err)
	}
	socket := &foreignPlainSocket{n: 1}
	s := &UDPServer{endpoint: &ServerEndpoint{router: r}, sockets: map[netip.AddrPort]net.PacketConn{route.local: socket}}
	w := newPacketWriter(r)
	defer w.close()
	result, err := w.offer(route, []byte{1}, netip.MustParseAddrPort("127.0.0.1:444"), recovery.NotECT, nil)
	if err != nil {
		t.Fatal(err)
	}
	route.revoke()
	request := <-w.writes
	n, err := s.writeForeignPacket(request)
	request.complete(n, err)
	if got := foreignResult(t, result); got.n != 0 || !errors.Is(got.err, ErrEndpointClosed) || socket.calls != 0 {
		t.Fatal("revoked queued write reached socket", got)
	}
}
