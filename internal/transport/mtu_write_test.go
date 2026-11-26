package transport

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"fmt"
	"github.com/getlantern/photon/internal/recovery"
	"io"
	"net/netip"
	"syscall"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func mtuWriteEndpoint(t *testing.T, version wire.Version) (*ServerEndpoint, *Conn) {
	t.Helper()
	e, c, _ := mtuWriteEndpointPair(t, version, 1500)
	return e, c
}

func mtuWriteEndpointPair(t *testing.T, version wire.Version, ceiling int) (*ServerEndpoint, *Conn, *Conn) {
	t.Helper()
	c, remote := sendingPathPair(t, version)
	local := netip.MustParseAddrPort("192.0.2.1:443")
	peer := netip.MustParseAddrPort("192.0.2.2:443")
	r, _ := NewRouter(8, 2)
	if err := r.Add(c, peer); err != nil {
		t.Fatal(err)
	}
	if _, err := c.multipath.beginAbandon(0); err != nil {
		t.Fatal(err)
	}
	if err := c.bindPreparedPath(1, local, peer); err != nil {
		t.Fatal(err)
	}
	app := c.multipath.paths[1].application
	app.validation.state = PathValidated
	app.budget.Validate()
	if err := c.startPathMTUSearch(1, ceiling); err != nil {
		t.Fatal(err)
	}
	return &ServerEndpoint{router: r, conns: []*Conn{c}, local: local, locals: map[netip.AddrPort]bool{local: true}, networks: map[*Conn]*networkOwner{c: {conn: c, primary: pathTuple{local: local, remote: peer}, activationChecked: true}}, readyNodes: make(map[*Conn]*list.Element), announced: make(map[*Conn]bool)}, c, remote
}

func TestMTUWriteRejection(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			e, c := mtuWriteEndpoint(t, version)
			// A rejected write mustn't cut the window, which only NewReno
			// would otherwise do.
			useNewReno(t, c)
			defer e.Close()
			now := time.Unix(101, 0)
			idleBefore := c.idle
			out, err := e.NextDatagram(now)
			if err != nil || out == nil || out.mtu == nil || len(out.Data) != 1350 {
				t.Fatalf("probe output: %+v %v", out, err)
			}
			app := c.multipath.paths[1].application
			window := app.congestion.Window()
			e.HandleWriteError(out, fmt.Errorf("write: %w", syscall.EMSGSIZE), now)
			if c.closed || len(e.conns) != 1 || app.sent.BytesInFlight != 0 || app.mtu.pending || app.mtu.upper != 1349 || app.congestion.Window() != window {
				t.Fatal("EMSGSIZE did not preserve connection and narrow search")
			}
			if c.idle != idleBefore {
				t.Fatal("failed probe changed idle activity")
			}
			if !app.timer.Deadline().IsZero() {
				t.Fatal("failed probe left PTO armed")
			}
			if app.mtu.acknowledged(out.mtu.number) {
				t.Fatal("rejected size remained confirmable")
			}
			out, err = e.NextDatagram(now.Add(time.Second))
			if err != nil || out == nil || len(out.Data) != 1275 || out.mtu == nil || out.mtu.number != 1 {
				t.Fatal("smaller fresh probe", err)
			}
			e.HandleWriteError(out, syscall.ECONNREFUSED, now)
			if !c.closed || len(e.conns) != 0 {
				t.Fatal("unrelated write error suppressed")
			}
		})
	}
}

func TestMTUWriteOrdinaryError(t *testing.T) {
	e, c := mtuWriteEndpoint(t, wire.Version1)
	defer e.Close()
	if err := c.queueFrameOnPath(1, wire.PingFrame{}); err != nil {
		t.Fatal(err)
	}
	out, err := e.NextDatagram(time.Unix(101, 0))
	if err != nil || out == nil || out.mtu != nil {
		t.Fatal("ordinary packet classified as MTU probe", err)
	}
	e.HandleWriteError(out, syscall.EMSGSIZE, time.Unix(101, 0))
	if !c.closed {
		t.Fatal("ordinary EMSGSIZE suppressed")
	}
}

func TestLargeStreamWriteRejection(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			e, c := mtuWriteEndpoint(t, version)
			// A rejected write mustn't cut the window, which only NewReno
			// would otherwise do.
			useNewReno(t, c)
			defer e.Close()
			a := c.multipath.paths[1].application
			// Establish a large confirmed limit; actual encrypted discovery is covered
			// separately. This test exercises the endpoint's committed write callback.
			a.mtu.confirmed, a.mtu.upper = 3000, 3000
			id, err := c.OpenStream(false)
			if err != nil {
				t.Fatal(err)
			}
			if n, err := c.WriteStream(id, make([]byte, 2500), true); err != nil || n != 2500 {
				t.Fatal(n, err)
			}
			c.queuedBytes = maxSendBytes
			now := time.Unix(101, 0)
			idleBefore := c.idle
			out, err := e.NextDatagram(now)
			if err != nil || out == nil || out.large == nil || len(out.Data) <= 1200 {
				t.Fatal("large output", err)
			}
			pn := out.large.number
			item := out.large.item
			window := a.congestion.Window()
			e.HandleWriteError(out, fmt.Errorf("socket: %w", syscall.EMSGSIZE), now)
			if c.closed || len(e.conns) != 1 || a.sent.BytesInFlight != 0 || a.congestion.Window() != window || c.idle != idleBefore {
				t.Fatal("rejection accounting")
			}
			if a.space.nextPN != pn+1 || a.space.sent[pn] != nil || !item.queued || c.queuedBytes != maxSendBytes {
				t.Fatal("retransmission ownership")
			}
			if a.mtu.confirmed != 1200 || !a.mtuNeedsValidation || a.mtuRaiseDeadline().IsZero() {
				t.Fatal("fallback state")
			}
			if _, err := a.sent.OnACK(recovery.Application, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: pn, Largest: pn}}}, now, 0, 0, true); !errors.Is(err, recovery.ErrACK) {
				t.Fatal("rejected packet ACK", err)
			}
			out, err = e.NextDatagram(now.Add(time.Second))
			if err != nil || out == nil || len(out.Data) != 1200 || a.validation.state != PathValidating || !item.queued {
				t.Fatal("baseline gate", err)
			}
			// Splitting must consume the precharged reserve even at full capacity.
			if err := c.splitQueuedStream(a.space, 0, portableStreamPayload); err != nil {
				t.Fatal(err)
			}
			if c.queuedBytes != maxSendBytes || len(a.space.queue) < 2 {
				t.Fatal("split reserve")
			}
		})
	}
}

// Packet exchange uses endpoint send/receive routing for the sender and a
// deterministic peer. Only the failed socket write is injected.
func TestEndpointLargeStreamWriteFallbackTransfer(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, dropChallenge := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/drop-challenge=%t", version, dropChallenge), func(t *testing.T) {
				e, c, peer := mtuWriteEndpointPair(t, version, 4800)
				defer e.Close()
				peer.multipath.paths[1].application.budget.Validate()
				peer.multipath.paths[1].application.validation.state = PathValidated
				a := c.multipath.paths[1].application
				now := time.Unix(101, 0)
				receive := func() {
					t.Helper()
					b, err := peer.nextPacketOnPath(1, now)
					if err != nil {
						t.Fatal(err)
					}
					if b != nil {
						got, reply, err := e.HandleDatagram(context.Background(), b, netip.MustParseAddrPort("192.0.2.2:443"), now)
						if err != nil || got != c || reply != nil {
							t.Fatal("endpoint receive", got == c, err)
						}
					}
				}
				probe, err := e.NextDatagram(now)
				if err != nil || probe == nil || len(probe.Data) != 3000 {
					t.Fatal("discovery", err)
				}
				if err := peer.handleMultipathPacket(probe.Data, now); err != nil {
					t.Fatal(err)
				}
				now = now.Add(time.Millisecond)
				ackNow(peer)
				receive()
				if a.mtu.confirmed != 3000 {
					t.Fatal("probe not confirmed")
				}
				id, err := c.OpenStream(false)
				if err != nil {
					t.Fatal(err)
				}
				payload := make([]byte, 2500)
				for i := range payload {
					payload[i] = byte(i * 37)
				}
				if n, err := c.WriteStream(id, payload, true); err != nil || n != len(payload) {
					t.Fatal(n, err)
				}
				now = now.Add(time.Millisecond)
				out, err := e.NextDatagram(now)
				if err != nil || out == nil || out.large == nil || len(out.Data) <= 1200 {
					t.Fatal("large STREAM", err)
				}
				rejected := out.large.number
				e.HandleWriteError(out, fmt.Errorf("socket: %w", syscall.EMSGSIZE), now)
				packets := 0
				dropped := false
				for i := 0; i < 300 && c.StreamAcknowledged(id) != nil; i++ {
					now = now.Add(time.Millisecond)
					out, err = e.NextDatagram(now)
					if err != nil {
						t.Fatal(err)
					}
					if out != nil {
						if len(out.Data) > 1200 {
							t.Fatal("oversized fallback", len(out.Data))
						}
						packets++
						if dropChallenge && !dropped {
							if a.validation.state != PathValidating || len(out.Data) != 1200 {
								t.Fatal("expected expanded challenge")
							}
							dropped = true
							now = e.Deadline()
							if now.IsZero() {
								t.Fatal("missing retry timer")
							}
							if err := e.HandleTimeout(now); err != nil {
								t.Fatal("retry timer", err)
							}
							continue
						}
						if err := peer.handleMultipathPacket(out.Data, now); err != nil {
							t.Fatal(err)
						}
					}
					now = now.Add(time.Millisecond)
					receive()
				}
				if dropped != dropChallenge {
					t.Fatal("drop not exercised")
				}
				if err := c.StreamAcknowledged(id); err != nil {
					t.Fatal("incomplete transfer", err)
				}
				if c.closed || a.mtuNeedsValidation || a.validation.state != PathValidated || packets < 4 || c.queuedBytes != 0 || a.sent.BytesInFlight != 0 {
					t.Fatal("completion accounting", packets, c.queuedBytes, a.sent.BytesInFlight)
				}
				if a.space.sent[rejected] != nil {
					t.Fatal("rejected transmission restored")
				}
				got := make([]byte, len(payload))
				if n, err := peer.ReadStream(id, got); n != len(payload) || !bytes.Equal(got, payload) {
					t.Fatal("reassembly", n, err)
				}
				var end [1]byte
				if n, err := peer.ReadStream(id, end[:]); n != 0 || !errors.Is(err, io.EOF) {
					t.Fatal("FIN", n, err)
				}
			})
		}
	}
}
