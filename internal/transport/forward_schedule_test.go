package transport

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestForwardScheduleAdvertisementGate(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, mode := range []string{"legacy", "base", "multipath"} {
			t.Run(fmt.Sprintf("%x/%s", uint32(version), mode), func(t *testing.T) {
				var c, peer *Conn
				if mode == "multipath" {
					maximum := uint64(3)
					c, peer = negotiatedPathPair(t, version, &maximum, &maximum)
					c.config.EnableMultipath = true
				} else {
					c, peer = testPair(t, version, streamParameters())
				}
				drivePair(t, c, peer, false, false)
				e, _ := NewClientOnlyEndpoint(8, 8)
				local, remote := netip.MustParseAddrPort("192.0.2.1:443"), netip.MustParseAddrPort("192.0.2.2:444")
				if err := e.router.Add(c, remote); err != nil {
					t.Fatal(err)
				}
				e.conns = append(e.conns, c)
				if mode != "legacy" {
					owner, err := newNetworkOwner(c, local, remote)
					if err != nil {
						t.Fatal(err)
					}
					if mode == "base" {
						if err := c.activateBasePath(local, remote); err != nil {
							t.Fatal(err)
						}
						owner.activationChecked = true
					}
					e.networks = map[*Conn]*networkOwner{c: owner}
					e.locals = map[netip.AddrPort]bool{local: true}
					e.local = local
				}
				frames, err := c.localIDs.issue(4)
				if err != nil || len(frames) == 0 {
					t.Fatal("missing fresh IDs", err)
				}
				for _, frame := range frames {
					if err := c.enqueue(applicationSpace, frame, 64); err != nil {
						t.Fatal(err)
					}
				}
				h := newPacketHandoff()
				defer h.close()
				e.router.handoff = h
				e.publicationWake = h.changed
				source, _ := NewRouter(8, 8)
				route := updateTestRoute(t, source, h, nil)
				if err := h.bindConnection(route, c); err != nil {
					t.Fatal(err)
				}
				space := &c.spaces[applicationSpace]
				beforePN, beforeBytes := space.nextPN, c.queuedBytes
				now := time.Unix(102, 0)
				for range 3 {
					out, err := e.NextDatagram(now)
					if err != nil || out != nil {
						t.Fatal("unreserved CID emitted", out, err)
					}
					now = now.Add(time.Millisecond)
				}
				if space.nextPN != beforePN {
					t.Fatal("held advertisement consumed packet number")
				}
				if mode != "multipath" && c.queuedBytes != beforeBytes {
					t.Fatal("held advertisement consumed queue bytes")
				}
				for _, frame := range frames {
					if c.localIDs.sent[frame.Sequence] {
						t.Fatal("unreserved ID marked sent")
					}
				}
				id, err := c.OpenStream(false)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := c.WriteStream(id, []byte("ordinary traffic"), true); err != nil {
					t.Fatal(err)
				}
				out, err := e.NextDatagram(now)
				if err != nil || out == nil {
					t.Fatal("ordinary traffic blocked", err)
				}
				if sent := space.sent[space.largestSent]; sent == nil {
					t.Fatal("stream not recorded")
				} else if _, ok := sent.frame.(wire.StreamFrame); !ok {
					t.Fatalf("unexpected frame %T", sent.frame)
				}
				source.applyForwardUpdates()
				// Activation must have preceded snapshot publication, including path IDs.
				for _, cid := range e.router.entries[c].ids {
					if source.foreignIDs[cid] != route {
						t.Fatal("snapshot omitted CID after activation")
					}
				}
				for step := 0; step < 30; step++ {
					now = now.Add(10 * time.Millisecond)
					if _, err := e.NextDatagram(now); err != nil {
						t.Fatal(err)
					}
				}
				for _, frame := range frames {
					if !c.localIDs.sent[frame.Sequence] {
						t.Fatal("acknowledged CID remained held")
					}
				}
				if mode == "multipath" {
					for pathID, state := range c.multipath.paths {
						if pathID == 0 {
							continue
						}
						for seq := range state.local.active {
							if !state.local.sent[seq] {
								t.Fatal("acknowledged path CID remained held")
							}
						}
					}
				}
			})
		}
	}
}

func TestForwardSchedulePTOBypassesHeldCID(t *testing.T) {
	for _, path := range []bool{false, true} {
		c, peer := testPair(t, wire.Version1, streamParameters())
		drivePair(t, c, peer, false, false)
		if path {
			if err := c.activateBasePath(netip.MustParseAddrPort("192.0.2.1:443"), netip.MustParseAddrPort("192.0.2.2:444")); err != nil {
				t.Fatal(err)
			}
		}
		c.forwardingBlocked = true
		frames, err := c.localIDs.issue(4)
		if err != nil || len(frames) == 0 {
			t.Fatal(err)
		}
		for _, frame := range frames {
			if err := c.enqueue(applicationSpace, frame, 64); err != nil {
				t.Fatal(err)
			}
		}
		var packet []byte
		if path {
			c.multipath.paths[0].application.probePending = true
			packet, err = c.nextPacketOnPath(0, time.Unix(102, 0))
		} else {
			c.probeAllowance[applicationSpace] = true
			packet, err = c.NextDatagram(time.Unix(102, 0))
		}
		if err != nil || len(packet) == 0 {
			t.Fatal("PTO blocked by CID", err)
		}
		for _, frame := range frames {
			if c.localIDs.sent[frame.Sequence] {
				t.Fatal("PTO advertised unreserved CID")
			}
		}
	}
}

func TestForwardScheduleAcknowledgementWakesRuntime(t *testing.T) {
	c, peer := testPair(t, wire.Version1, streamParameters())
	drivePair(t, c, peer, false, false)
	sink, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	e, _ := NewClientOnlyEndpoint(8, 8)
	if err := e.AddClient(c, sink.LocalAddr().(*net.UDPAddr).AddrPort()); err != nil {
		t.Fatal(err)
	}
	destination, err := NewUDPServerBorrowed(socket, e)
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	h, err := destination.handoffTarget(ctx)
	if err != nil {
		t.Fatal(err)
	}
	source, _ := NewRouter(8, 8)
	wake := make(chan struct{}, 1)
	route := &handoffRoute{router: source, target: h, wake: wake}
	if !h.subscribe(route) {
		t.Fatal("subscribe")
	}
	source.forwardRegistrations = map[*handoffRoute]struct{}{route: {}}
	if err := destination.bindHandoffConnection(ctx, route, c); err != nil {
		t.Fatal(err)
	}
	select {
	case <-wake:
	case <-ctx.Done():
		t.Fatal("initial snapshot not queued")
	}
	source.applyForwardUpdates()
	if err := destination.Wait(ctx, func(e *ServerEndpoint) (bool, error) { return e.router.publishForwardCIDs(c, e.publicationWake) }); err != nil {
		t.Fatal(err)
	}
	if err := destination.Do(ctx, func(*ServerEndpoint) error {
		frames, err := c.localIDs.issue(4)
		if err != nil {
			return err
		}
		for _, frame := range frames {
			if err := c.enqueue(applicationSpace, frame, 64); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-wake:
	case <-ctx.Done():
		t.Fatal("replacement snapshot not queued")
	}
	var data [1500]byte
	sink.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
	if n, _, err := sink.ReadFromUDP(data[:]); err == nil || n != 0 {
		t.Fatal("advertisement sent before acknowledgement", n, err)
	} else if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		t.Fatal(err)
	}
	source.applyForwardUpdates()
	// No destination command follows the ACK: the notification itself must send.
	sink.SetReadDeadline(time.Now().Add(time.Second))
	if n, _, err := sink.ReadFromUDP(data[:]); err != nil || n == 0 {
		t.Fatal("acknowledgement did not resume output", n, err)
	}
}

func TestForwardScheduleDatagramShrink(t *testing.T) {
	for _, path := range []bool{false, true} {
		var c *Conn
		if path {
			c, _ = sizedDatagramPair(t, wire.Version1)
		} else {
			params := streamParameters()
			params.MaxDatagramFrameSize = 65535
			var peer *Conn
			c, peer = testPair(t, wire.Version1, params)
			drivePair(t, c, peer, false, false)
			c.config.InitialPacketSize = 1452
		}
		frames, err := c.localIDs.issue(4)
		if err != nil || len(frames) == 0 {
			t.Fatal(err)
		}
		for _, frame := range frames {
			if err := c.enqueue(applicationSpace, frame, 64); err != nil {
				t.Fatal(err)
			}
		}
		if err := c.SendDatagram(make([]byte, 1410)); err != nil {
			t.Fatal(err)
		}
		if err := c.SendDatagram([]byte("fits")); err != nil {
			t.Fatal(err)
		}
		c.forwardingBlocked = true
		var packet []byte
		if path {
			c.multipath.paths[0].application.resetMTU()
			packet, err = c.nextPacketOnPath(0, time.Unix(102, 0))
		} else {
			c.config.InitialPacketSize = 1200
			packet, err = c.NextDatagram(time.Unix(102, 0))
		}
		if err != nil || len(packet) == 0 {
			t.Fatal("held CID blocked datagram shrink", err)
		}
		if c.pendingDatagrams != 0 {
			t.Fatal("datagram not sent/dropped")
		}
		for _, frame := range frames {
			if c.localIDs.sent[frame.Sequence] {
				t.Fatal("datagram cleanup exposed unreserved CID")
			}
		}
	}
}

func TestForwardScheduleFailedAlternative(t *testing.T) {
	c, peer := testPair(t, wire.Version1, streamParameters())
	drivePair(t, c, peer, false, false)
	e, _ := NewClientOnlyEndpoint(8, 8)
	if err := e.AddClient(c, netip.MustParseAddrPort("192.0.2.1:443")); err != nil {
		t.Fatal(err)
	}
	h := newPacketHandoff()
	defer h.close()
	e.router.handoff = h
	source, _ := NewRouter(8, 8)
	route := updateTestRoute(t, source, h, nil)
	updateTestRoute(t, source, h, [][]byte{c.config.LocalCID})
	if err := h.bindConnection(route, c); err != nil {
		t.Fatal(err)
	}
	frames, err := c.localIDs.issue(4)
	if err != nil {
		t.Fatal(err)
	}
	for _, frame := range frames {
		if err := c.enqueue(applicationSpace, frame, 64); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Unix(102, 0)
	if out, err := e.NextDatagram(now); out != nil || err != nil {
		t.Fatal(out, err)
	}
	source.applyForwardUpdates() // Collides with the other registration.
	id, err := c.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.WriteStream(id, []byte("primary survives"), true); err != nil {
		t.Fatal(err)
	}
	if out, err := e.NextDatagram(now); out == nil || err != nil {
		t.Fatal("failed alternative blocked primary stream", err)
	}
	if c.closed || c.Err() != nil || !c.forwardingBlocked {
		t.Fatal("publication failure lifecycle")
	}
	source.removeForwardRoute(route)
	for range 10 {
		now = now.Add(10 * time.Millisecond)
		if _, err := e.NextDatagram(now); err != nil {
			t.Fatal(err)
		}
	}
	for _, frame := range frames {
		if !c.localIDs.sent[frame.Sequence] {
			t.Fatal("removing failed alternative did not release advertisements")
		}
	}
}

func TestForwardScheduleSharedCapacityWakeup(t *testing.T) {
	for _, revoke := range []bool{false, true} {
		c, _ := testPair(t, wire.Version1)
		owner, _ := NewRouter(8, 8)
		if err := owner.Add(c, netip.MustParseAddrPort("192.0.2.1:443")); err != nil {
			t.Fatal(err)
		}
		first, second := newPacketHandoff(), newPacketHandoff()
		defer first.close()
		defer second.close()
		owner.handoff = second
		source, _ := NewRouter(8, 8)
		a, b := updateTestRoute(t, source, first, nil), updateTestRoute(t, source, second, nil)
		if err := second.bindConnection(b, c); err != nil {
			t.Fatal(err)
		}
		<-second.changed // Discard the binding notification.
		ids := make([][]byte, maxForwardCIDs)
		for i := range ids {
			ids[i] = []byte{1}
		}
		if _, err := a.queueCIDUpdate(ids, nil); err != nil {
			t.Fatal(err)
		}
		if ready, err := owner.publishForwardCIDs(c, second.changed); ready || err != ErrRouterCapacity {
			t.Fatal("expected pending capacity pressure", ready, err)
		}
		if revoke {
			a.revoke()
			source.pruneForwardRoutes()
		}
		source.applyForwardUpdates()
		select {
		case <-second.changed:
		default:
			t.Fatal("capacity release did not wake another destination")
		}
		if ready, err := owner.publishForwardCIDs(c, second.changed); ready || err != nil {
			t.Fatal("capacity not reusable", ready, err)
		}
		source.applyForwardUpdates()
		if ready, err := owner.publishForwardCIDs(c, second.changed); !ready || err != nil {
			t.Fatal("publication did not recover", ready, err)
		}
	}
}
