package transport

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/getlantern/photon/internal/recovery"
)

type serverCommand struct {
	ctx    context.Context
	run    func(*ServerEndpoint) error
	notify bool
	reply  chan serverResult
}
type serverResult struct {
	err     error
	changed <-chan struct{}
}

// A buffer belongs to exactly one reader or queued datagram until the owner
// finishes HandleDatagram. Deferred packets and application data own copies.
var udpReadBuffers = sync.Pool{New: func() any { return new([65535]byte) }}

type receivedUDP struct {
	resetReturn   *resetReturnTicket // optional bounded reply to the original socket owner
	resetFallback bool               // source-owned authentication failed before reset handoff
	route         *handoffRoute      // immutable provenance for cross-owner packets
	forwarded     bool               // No CID redispatch; optional return then one final reset handoff.
	marking       ECN
	local         netip.AddrPort
	buffer        *[65535]byte
	data          []byte
	peer          netip.AddrPort
	err           error
}

var errOperationWouldBlock = errors.New("quic: runtime operation would block")

// UDPServer owns UDP sockets and a serialized server or dedicated client endpoint. Conn operations
// must run through Do, never concurrently on returned pointers. Callbacks must
// finish promptly and must not call back into UDPServer. This is an internal
// runtime backing the public concurrent connection/stream API.
type UDPServer struct {
	socketMu               sync.Mutex
	packets                chan receivedUDP
	resetReturns           *resetReturnMailbox
	returned               <-chan resetReturnResult
	handoff                *packetHandoff // receiver owner only
	forwarded              <-chan receivedUDP
	routesChanged          chan struct{}
	publicationChanged     <-chan struct{}
	foreignWriter          *packetWriter
	foreignWrites          <-chan *foreignWrite
	sockets                map[netip.AddrPort]net.PacketConn
	readers                atomic.Int32
	watches                map[*Conn]connectionWatch
	socket                 *net.UDPConn
	packetConn             net.PacketConn
	primaryAddress         netip.AddrPort
	borrowedSocket         net.PacketConn
	endpoint               *ServerEndpoint
	commands               chan serverCommand
	stop, done, readerDone chan struct{}
	once                   sync.Once
	nonQUIC                *nonQUICQueue
}

// NewUDPServer transfers exclusive ownership of socket and endpoint to the
// runtime. Close closes both and waits for its two goroutines to terminate.
func NewUDPServer(socket *net.UDPConn, endpoint *ServerEndpoint) (*UDPServer, error) {
	return newMultiUDPServer([]*net.UDPConn{socket}, endpoint)
}

// NewUDPServerWithSockets transfers all supplied sockets to one serialized owner.
// On error, socket ownership remains with the caller.
func NewUDPServerWithSockets(sockets []*net.UDPConn, endpoint *ServerEndpoint) (*UDPServer, error) {
	return newMultiUDPServer(sockets, endpoint)
}

// NewUDPServerBorrowed exclusively uses a caller-owned UDP socket until Close or
// Done. The caller must not read from it or manage its deadlines in that period.
// Shutdown interrupts I/O, joins all readers and clears deadlines without closing
// the borrowed socket. Additional sockets opened by this runtime remain owned.
// Constructor failure leaves the endpoint and socket with the caller.
func NewUDPServerBorrowed(socket *net.UDPConn, endpoint *ServerEndpoint) (*UDPServer, error) {
	return newUDPServerWithOwnership([]*net.UDPConn{socket}, endpoint, true)
}

// newMultiUDPServer transfers a fixed socket set to one owner goroutine, with
// one reader per socket. A shared endpoint router reserves CIDs across the whole
// set. Constructor failure leaves ownership with the caller; Close joins all
// readers. Failed sockets retire their paths; surviving socket paths can continue.
func newMultiUDPServer(sockets []*net.UDPConn, endpoint *ServerEndpoint) (*UDPServer, error) {
	return newUDPServerWithOwnership(sockets, endpoint, false)
}

func newUDPServerWithOwnership(sockets []*net.UDPConn, endpoint *ServerEndpoint, borrowed bool) (*UDPServer, error) {
	conns := make([]net.PacketConn, len(sockets))
	for i, socket := range sockets {
		conns[i] = socket
	}
	return newPacketServerWithOwnership(conns, endpoint, borrowed)
}

// NewPacketServerBorrowed exclusively uses a caller-owned packet connection.
// ReadFrom addresses must represent UDP IP/port pairs. Without ancillary socket
// capabilities, ECN and kernel-dependent MTU probing are disabled. Shutdown follows
// NewUDPServerBorrowed's deadline and ownership contract.
func NewPacketServerBorrowed(socket net.PacketConn, endpoint *ServerEndpoint) (*UDPServer, error) {
	return newPacketServerWithOwnership([]net.PacketConn{socket}, endpoint, true)
}

func newPacketServerWithOwnership(sockets []net.PacketConn, endpoint *ServerEndpoint, borrowed bool) (*UDPServer, error) {
	if len(sockets) == 0 || len(sockets) > 64 {
		return nil, errors.New("quic: invalid socket count")
	}
	socket := sockets[0]
	if nilPacketConn(socket) || endpoint == nil || endpoint.closed {
		return nil, errors.New("quic: invalid UDP server")
	}
	capable := endpoint.config.EnableMultipath || endpoint.config.multipathCapacity > 0
	// A borrowed socket can gain clients/listeners after startup. Prepare its
	// concrete address owner now, before any connection activates paths.
	migration := endpoint.config.EnableMigration || borrowed
	for _, c := range endpoint.conns {
		capable = capable || c.config.multipathCapacity > 0
		migration = migration || c.config.EnableMigration
	}
	socketMap := make(map[netip.AddrPort]net.PacketConn)
	allOOB := true
	var addresses []netip.AddrPort
	for _, sock := range sockets {
		if nilPacketConn(sock) {
			return nil, errors.New("quic: nil socket")
		}
		addr, err := packetAddrPort(sock.LocalAddr())
		if err != nil {
			return nil, err
		}
		_, capable := sock.(OOBCapablePacketConn)
		allOOB = allOOB && capable
		if socketMap[addr] != nil {
			return nil, errors.New("quic: duplicate socket")
		}
		socketMap[addr] = sock
		addresses = append(addresses, addr)
	}
	if len(sockets) > 1 && !capable && !migration {
		return nil, errors.New("quic: multiple sockets require address-aware owner")
	}
	for _, sock := range sockets {
		if udp, ok := sock.(OOBCapablePacketConn); ok {
			if err := configureUDPSocket(udp); err != nil {
				return nil, err
			}
			configureReceiveECN(udp)
		}
	}
	if capable || (migration && !addresses[0].Addr().IsUnspecified()) {
		if err := endpoint.bindNetworkOwner(addresses...); err != nil {
			return nil, err
		}
	}
	if len(endpoint.config.PreferredAddresses) > 0 && !endpoint.config.preferredSocketsReady {
		return nil, errors.New("quic: preferred addresses require concrete registered sockets")
	}
	endpoint.config.enablePathMTUDiscovery = endpoint.networks != nil && udpSupportsMTUDiscovery && allOOB
	endpoint.config.enableECN = udpSupportsECN && allOOB
	for _, c := range endpoint.conns {
		c.config.enablePathMTUDiscovery = endpoint.config.enablePathMTUDiscovery
		c.config.enableECN = endpoint.config.enableECN
	}
	s := &UDPServer{sockets: socketMap, packetConn: socket, primaryAddress: addresses[0], endpoint: endpoint, commands: make(chan serverCommand), stop: make(chan struct{}), done: make(chan struct{}), readerDone: make(chan struct{})}
	s.socket, _ = socket.(*net.UDPConn)
	if borrowed {
		s.borrowedSocket = socket
	}
	s.watches = make(map[*Conn]connectionWatch)
	s.endpoint.onTerminal = func(c *Conn, err error) {
		if watch, ok := s.watches[c]; ok {
			watch.cancel(err)
			delete(s.watches, c)
		}
	}
	packets := make(chan receivedUDP, 64)
	s.packets = packets
	s.readers.Store(int32(len(sockets)))
	for local, sock := range s.sockets {
		go s.readSocket(sock, local, packets)
	}
	go s.run(packets)
	return s, nil
}
func (s *UDPServer) Addr() net.Addr { return s.packetConn.LocalAddr() }
func (s *UDPServer) Close() error {
	s.stopRuntime()
	<-s.done
	<-s.readerDone
	return nil
}
func (s *UDPServer) readSocket(socket net.PacketConn, local netip.AddrPort, packets chan<- receivedUDP) {
	defer func() {
		s.socketMu.Lock()
		defer s.socketMu.Unlock()
		if s.readers.Add(-1) == 0 {
			close(s.readerDone)
		}
	}()
	var oob [128]byte
	udp, _ := socket.(*net.UDPConn)
	controlSocket, _ := socket.(OOBCapablePacketConn)
	for {
		buffer := udpReadBuffers.Get().(*[65535]byte)
		var n, oobn, flags int
		var peer netip.AddrPort
		var err error
		if udp != nil {
			n, oobn, flags, peer, err = udp.ReadMsgUDPAddrPort(buffer[:], oob[:])
		} else {
			var address net.Addr
			if controlSocket != nil {
				var udpAddress *net.UDPAddr
				n, oobn, flags, udpAddress, err = controlSocket.ReadMsgUDP(buffer[:], oob[:])
				address = udpAddress
			} else {
				n, address, err = socket.ReadFrom(buffer[:])
			}
			if err == nil {
				if n < 0 || n > len(buffer) || oobn < 0 || oobn > len(oob) {
					err = errors.New("quic: invalid PacketConn read length")
				} else {
					var addressErr error
					peer, addressErr = packetAddrPort(address)
					if addressErr != nil {
						udpReadBuffers.Put(buffer)
						continue
					}
				}
			}
		}
		// Some platforms return negative lengths when the socket is closed.
		if err != nil {
			n, oobn, flags = 0, 0, 0
		}
		if udpPayloadTruncated(flags) {
			n = 0
		}
		marking := receivedECN(oob[:oobn], flags)
		received := receivedUDP{marking: marking, local: local, buffer: buffer, data: buffer[:n], peer: peer, err: err}
		select {
		case packets <- received:
		case <-s.stop:
			udpReadBuffers.Put(buffer)
			return
		case <-s.done:
			udpReadBuffers.Put(buffer)
			return
		}
		if err != nil {
			return
		}
	}
}
func (s *UDPServer) run(packets <-chan receivedUDP) {
	defer close(s.done)
	defer func() {
		if s.resetReturns != nil {
			s.resetReturns.close()
		}
		if s.foreignWriter != nil {
			s.foreignWriter.close()
		}
		for route := range s.endpoint.router.forwardRegistrations {
			s.endpoint.router.removeForwardRoute(route)
		}
		if s.handoff != nil {
			s.handoff.close()
		}
		s.stopRuntime()
		<-s.readerDone
		// No producers remain; release buffers queued before shutdown.
		for len(s.packets) > 0 {
			packet := <-s.packets
			udpReadBuffers.Put(packet.buffer)
		}
		s.nonQUIC = nil
		if s.borrowedSocket != nil {
			_ = s.borrowedSocket.SetDeadline(time.Time{})
		}
	}()
	defer s.endpoint.Close()
	lifetime, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Only blocked operations need a notification channel. Registration and
	// signaling both happen on this goroutine, so a wakeup cannot race registration.
	var changed chan struct{}
	signal := func() {
		if changed != nil {
			close(changed)
			changed = nil
		}
	}
	defer signal()
	var outbound Outbound
	var baseStorage outboundBaseLarge
	// Typical MTU packets reuse this storage; larger packets allocate in SealInto.
	var packetStorage [2048]byte
	defer clear(packetStorage[:])
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		if s.endpoint.closed || (s.endpoint.closeWhenIdle && len(s.endpoint.conns) == 0) {
			return
		}
		// Bound each send batch so a busy connection cannot starve commands/input.
		more := false
		for i := 0; i < 64; i++ {
			out, err := s.endpoint.nextDatagramInto(time.Now(), &outbound, packetStorage[:], &baseStorage)
			if err != nil {
				if i == 63 {
					more = true
				}
				signal()
				continue
			} // Failed connections are removed by endpoint.
			if out == nil {
				break
			}
			if _, err := s.writeOutbound(out); err != nil {
				s.endpoint.HandleWriteError(out, err, time.Now())
				signal()
			}
			outbound = Outbound{} // Release packet/connection references before waiting.
			baseStorage = outboundBaseLarge{}
			if i == 63 {
				more = true
			}
		}
		if s.endpoint.closeWhenIdle && len(s.endpoint.conns) == 0 {
			return
		}
		var wake <-chan time.Time
		deadline := s.endpoint.Deadline()
		if more {
			deadline = time.Now()
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		if !deadline.IsZero() {
			timer.Reset(max(time.Until(deadline), 0))
			wake = timer.C
		}
		select {
		case <-s.stop:
			return
		case p := <-packets:
			if !s.handleReceived(lifetime, p) {
				return
			}
			signal()
		case p := <-s.forwarded:
			if !s.handleReceived(lifetime, p) {
				return
			}
			signal()
		case result := <-s.returned:
			s.handleResetReturn(result, time.Now())
			signal()
		case <-s.publicationChanged:
			signal()
		case request := <-s.foreignWrites:
			s.foreignWriter.capacityAvailable()
			n, err := s.writeForeignPacket(request)
			request.complete(n, err)
		case <-s.routesChanged:
			s.endpoint.router.pruneForwardRoutes()
			if s.foreignWriter != nil {
				s.foreignWriter.pruneReservations()
			}
			s.endpoint.router.applyForwardUpdates()
			signal()
		case cmd := <-s.commands:
			err := cmd.ctx.Err()
			if err == nil {
				err = cmd.run(s.endpoint)
				if cmd.notify && !errors.Is(err, errOperationWouldBlock) {
					signal()
				}
			}
			var wait <-chan struct{}
			if errors.Is(err, errOperationWouldBlock) || errors.Is(err, ErrAcceptWouldBlock) {
				if changed == nil {
					changed = make(chan struct{})
				}
				wait = changed
			}
			cmd.reply <- serverResult{err: err, changed: wait}
		case now := <-wake:
			_ = s.endpoint.HandleTimeout(now)
			signal()
		}
	}
}

// handleReceived consumes one owned buffer, whether read locally or handed off.
// Datagram parsing, authentication and side effects remain on this owner.
func (s *UDPServer) handleReceived(ctx context.Context, p receivedUDP) bool {
	if p.err == nil && !p.forwarded {
		route := s.endpoint.router.forwardRoute(p.data)
		cidSelected := route != nil
		if route == nil && len(s.endpoint.router.forwardRegistrations) != 0 {
			if tuple, err := canonicalPathTuple(p.local, p.peer); err == nil {
				if _, known := s.endpoint.router.routeIdentity(p.data, tuple, true); !known {
					// No CID owner can authenticate this packet. Select across both
					// ownership domains before either can consume a reset token.
					candidate, ambiguous := s.resetSelection(p, time.Now())
					if ambiguous {
						udpReadBuffers.Put(p.buffer)
						return true
					}
					route = candidate.route
				}
			}
		}
		if route != nil {
			if !p.forwarded && p.local == route.local {
				if cidSelected {
					p.resetReturn = s.reserveResetReturn(p, route, time.Now())
				}
				p.forwarded = true
				p.route = route
				p.resetFallback = !cidSelected
				if route.target.offer(p) {
					return true
				}
			}
			if p.resetReturn != nil {
				p.resetReturn.cancel()
			}
			// A reserved foreign CID never falls through to raw delivery, new
			// connection admission or a stateless reset when forwarding fails.
			udpReadBuffers.Put(p.buffer)
			return true
		}
	}
	release := true
	defer func() {
		if release {
			udpReadBuffers.Put(p.buffer)
		}
	}()
	if p.err != nil {
		for route := range s.endpoint.router.forwardRegistrations {
			if route.local == p.local {
				s.endpoint.router.removeForwardRoute(route)
			}
		}
		s.endpoint.failLocalSocket(p.local, p.err)
		return len(s.endpoint.locals) != 0
	}
	if p.forwarded {
		stopFallback := s.endpoint.handleForwardedDatagram(p, time.Now())
		if ticket := p.resetReturn; ticket != nil {
			p.resetReturn = nil
			if ticket.complete(p, stopFallback) {
				release = false
			}
		}
		return true
	}
	if s.captureNonQUIC(p.data, p.local, p.peer) {
		return true
	}
	before := s.endpoint.authenticatedDatagrams
	_, response, receiveErr := s.endpoint.handleNetworkDatagramECN(ctx, p.data, p.local, p.peer, time.Now(), p.marking)
	if s.endpoint.authenticatedDatagrams == before && !errors.Is(receiveErr, ErrStatelessReset) {
		if route := s.endpoint.router.foreignResetMatch(p.data, p.local, p.peer); route != nil {
			p.forwarded, p.resetFallback, p.route = true, true, route
			if route.target.offer(p) {
				release = false
			}
			// Matched reset hints never fall through to stateless reset output.
			return true
		}
	}
	if response != nil {
		_, _ = s.writeOutbound(response)
	}
	return true
}

func (s *UDPServer) invoke(ctx context.Context, run func(*ServerEndpoint) error, notify bool) (serverResult, error) {
	cmd := serverCommand{ctx: ctx, run: run, notify: notify, reply: make(chan serverResult, 1)}
	return s.submit(cmd)
}

func (s *UDPServer) submit(cmd serverCommand) (serverResult, error) {
	select {
	case <-cmd.ctx.Done():
		return serverResult{}, cmd.ctx.Err()
	case <-s.done:
		return serverResult{}, ErrEndpointClosed
	case s.commands <- cmd:
	}
	// Once submitted, receive the outcome even if cancellation races execution.
	// Otherwise Accept could consume a connection and then lose it to cancellation.
	select {
	case result := <-cmd.reply:
		return result, result.err
	case <-s.done:
		// A command that itself stops the endpoint replies before done closes;
		// both are then ready, and its outcome must win.
		select {
		case result := <-cmd.reply:
			return result, result.err
		default:
			return serverResult{}, ErrEndpointClosed
		}
	}
}

// Do executes one bounded operation on the socket's owner goroutine. The
// callback must not retain mutable endpoint state for access outside Do.
func (s *UDPServer) Do(ctx context.Context, run func(*ServerEndpoint) error) error {
	_, err := s.invoke(ctx, run, true)
	return err
}

// Wait retries a nonblocking operation when endpoint activity might allow it
// to progress. The callback runs on the owner goroutine and returns ready=false
// only if it made no progress and needs another event. It must not retain caller
// buffers for later use. Failed readiness checks do not wake other waiters,
// preventing blocked operations from repeatedly waking each other.
func (s *UDPServer) Wait(ctx context.Context, run func(*ServerEndpoint) (ready bool, err error)) error {
	cmd := serverCommand{ctx: ctx, notify: true, reply: make(chan serverResult, 1),
		run: func(e *ServerEndpoint) error {
			ready, err := run(e)
			if err != nil {
				return err
			}
			if !ready {
				return errOperationWouldBlock
			}
			return nil
		},
	}
	for {
		// Each retry follows receipt of the previous reply. A closed endpoint
		// ends this Wait, so its private channel cannot reach another operation.
		result, err := s.submit(cmd)
		if !errors.Is(err, errOperationWouldBlock) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.done:
			return ErrEndpointClosed
		case <-result.changed:
		}
	}
}
func (s *UDPServer) Accept(ctx context.Context) (*Conn, netip.AddrPort, error) {
	return s.acceptListener(ctx, 0)
}

// AcceptListener waits only for the listener identified by generation. A close
// followed by a restart cannot redirect a blocked call to the new listener.
func (s *UDPServer) AcceptListener(ctx context.Context, generation uint64) (*Conn, netip.AddrPort, error) {
	if generation == 0 {
		return nil, netip.AddrPort{}, ErrListenerClosed
	}
	return s.acceptListener(ctx, generation)
}

func (s *UDPServer) acceptListener(ctx context.Context, generation uint64) (*Conn, netip.AddrPort, error) {
	for {
		var c *Conn
		var peer netip.AddrPort
		result, err := s.invoke(ctx, func(e *ServerEndpoint) error {
			var err error
			if generation == 0 {
				c, peer, err = e.Accept()
			} else {
				c, peer, err = e.AcceptListener(generation)
			}
			return err
		}, false)
		if !errors.Is(err, ErrAcceptWouldBlock) {
			return c, peer, err
		}
		select {
		case <-ctx.Done():
			return nil, netip.AddrPort{}, ctx.Err()
		case <-s.done:
			return nil, netip.AddrPort{}, ErrEndpointClosed
		case <-result.changed:
		}
	}
}

type connectionWatch struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
}

// ConnectionContext obtains an owner-managed lifetime context for an admitted
// connection. It is canceled on connection failure, local close, or shutdown.
func (s *UDPServer) ConnectionContext(ctx context.Context, c *Conn) (context.Context, error) {
	var lifetime context.Context
	_, err := s.invoke(ctx, func(e *ServerEndpoint) error {
		if c.lifetimeContext != nil {
			lifetime = c.lifetimeContext
			return nil
		}
		if watch, ok := s.watches[c]; ok {
			lifetime = watch.ctx
			return nil
		}
		var cancel context.CancelCauseFunc
		lifetime, cancel = context.WithCancelCause(context.Background())
		if c.err != nil {
			cancel(c.err)
			return nil
		}
		if _, ok := e.router.entries[c]; !ok || c.closed {
			cancel(ErrEndpointClosed)
			return nil
		}
		s.watches[c] = connectionWatch{lifetime, cancel}
		return nil
	}, false)
	return lifetime, err
}

// Done is closed after endpoint disposal, reader shutdown and borrowed socket release.
func (s *UDPServer) Done() <-chan struct{} { return s.done }

func (s *UDPServer) stopRuntime() {
	s.once.Do(func() { close(s.stop); s.closeSockets() })
}

func (s *UDPServer) closeSockets() {
	s.socketMu.Lock()
	defer s.socketMu.Unlock()
	for address, socket := range s.sockets {
		if s.borrowedSocket != nil && address == s.primaryAddress {
			_ = socket.SetDeadline(time.Now())
		} else {
			socket.Close()
		}
	}
}
func (s *UDPServer) writeOutbound(out *Outbound) (int, error) {
	connection := s.packetConn
	if connection == nil {
		connection = s.socket
	}
	if out.Local.IsValid() {
		connection = s.sockets[canonicalPeer(out.Local)]
	}
	if connection == nil {
		return 0, errPathBinding
	}
	socket, udp := connection.(*net.UDPConn)
	if udp && socket == nil {
		return 0, errPathBinding
	}
	if !udp {
		if controlSocket, ok := connection.(OOBCapablePacketConn); ok {
			return writeWithECNFallback(out, func(marking recovery.ECNMark) (int, error) {
				if marking == recovery.NotECT {
					return connection.WriteTo(out.Data, net.UDPAddrFromAddrPort(out.Peer))
				}
				return writeECNDatagram(controlSocket, out.Data, out.Peer, marking)
			})
		}
		if out.ECN != recovery.NotECT {
			if out.ecn.sent == nil || !out.ecn.sent.RejectECNMarking(out.ecn.space, out.ecn.number) {
				return 0, errors.New("quic: ECN marking on generic PacketConn")
			}
			out.ECN = recovery.NotECT
		}
		return connection.WriteTo(out.Data, net.UDPAddrFromAddrPort(out.Peer))
	}
	if out.ECN != recovery.NotECT {
		return writeWithECNFallback(out, func(marking recovery.ECNMark) (int, error) {
			if marking == recovery.NotECT {
				return socket.WriteToUDPAddrPort(out.Data, out.Peer)
			}
			return writeECNDatagram(socket, out.Data, out.Peer, marking)
		})
	}
	return socket.WriteToUDPAddrPort(out.Data, out.Peer)
}
