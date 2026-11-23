package transport

import (
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func scheduleForeignWrite(t *testing.T, out *Outbound) (*pendingForeignWrite, *foreignWrite) {
	t.Helper()
	r, _ := NewRouter(8, 4)
	w := newPacketWriter(r)
	t.Cleanup(w.close)
	ticket, err := w.reserve(&handoffRoute{router: r, target: newPacketHandoff()})
	if err != nil {
		t.Fatal(err)
	}
	p, err := beginForeignWrite(ticket, out, nil)
	if err != nil {
		t.Fatal(err)
	}
	return p, <-w.writes
}

func TestForeignWriteSchedulerBlocksConstruction(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, kind := range []string{"base-stream", "stream"} {
			t.Run(fmt.Sprintf("%x/%s", uint32(v), kind), func(t *testing.T) {
				e, c, _, out := ownedOutboundFixture(t, v, kind)
				p, request := scheduleForeignWrite(t, out)
				id, err := c.OpenStream(false)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := c.WriteStream(id, []byte("next"), true); err != nil {
					t.Fatal(err)
				}
				sealed := c.sealed
				now := time.Unix(102, 0)
				for range 3 {
					next, err := e.NextDatagram(now)
					if err != nil || next != nil || c.sealed != sealed || c.pendingWrite != p {
						t.Fatal("constructed before completion", err)
					}
					if c.multipath == nil {
						data, err := c.NextDatagram(now)
						if err != nil || len(data) != 0 {
							t.Fatal("direct construction bypassed hold", err)
						}
					} else {
						_, _, data, err := c.nextBoundMultipathPacket(now)
						if err != nil || len(data) != 0 {
							t.Fatal("path construction bypassed hold", err)
						}
					}
				}
				request.complete(len(request.data), nil)
				next, err := e.NextDatagram(now)
				if err != nil || next == nil || c.pendingWrite != nil || !p.done || c.sealed == sealed {
					t.Fatal("completion failed to resume", err)
				}
			})
		}
	}
}

func TestForeignWriteSchedulerWriteRejection(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, partial := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/partial=%v", uint32(v), partial), func(t *testing.T) {
				e, c, _, out := ownedOutboundFixture(t, v, "probe")
				app := c.multipath.paths[1].application
				p, request := scheduleForeignWrite(t, out)
				n := 0
				if partial {
					n = 1
				}
				request.complete(n, syscall.EMSGSIZE)
				next, err := e.NextDatagram(time.Unix(102, 0))
				if err != nil || !p.done || c.pendingWrite != nil {
					t.Fatal("completion not consumed", err)
				}
				if partial {
					if !c.closed || next != nil || app.mtu.upper != 1500 {
						t.Fatal("partial send treated as unsent")
					}
				} else if c.closed || next == nil || app.mtu.upper != len(request.data)-1 {
					t.Fatal("missing atomic PMTU fallback")
				}
			})
		}
	}
}

func TestForeignWritePendingLifetimeTimers(t *testing.T) {
	for _, kind := range []string{"base-stream", "stream"} {
		t.Run(kind, func(t *testing.T) {
			e, c, _, out := ownedOutboundFixture(t, wire.Version1, kind)
			c.config.Parameters.MaxIdleTimeout = 60000
			c.peerParams.MaxIdleTimeout = 60000
			c.config.KeepAlivePeriod = time.Millisecond
			c.idle.sentSinceReceive = false
			c.pacingAt = time.Unix(100, 0)
			if c.multipath != nil {
				c.multipath.paths[1].application.pacingAt = c.pacingAt
			}
			before := c.RecoveryDeadline()
			p, request := scheduleForeignWrite(t, out)
			idle := c.IdleDeadline()
			if before.IsZero() || !before.Before(idle) || c.RecoveryDeadline() != idle || e.Deadline() != idle {
				t.Fatal("send deadline not suppressed", before, c.RecoveryDeadline(), idle)
			}
			c.earlyReadExpiry = time.Unix(102, 0)
			c.applicationKeys.previous = c.readKeys(applicationSpace)
			c.applicationKeys.previousExpiry = time.Unix(102, 0)
			if e.Deadline() != c.earlyReadExpiry {
				t.Fatal("key expiry suppressed")
			}
			if err := e.HandleTimeout(time.Unix(102, 0)); err != nil {
				t.Fatal(err)
			}
			if c.applicationKeys.previous != nil || !c.earlyReadExpiry.IsZero() || e.Deadline() != idle || c.pendingWrite != p {
				t.Fatal("lifetime service did not advance deadline")
			}
			// Even an explicit timeout call must not retire pending send recovery state.
			if err := c.HandleTimeout(time.Unix(103, 0)); err != nil {
				t.Fatal(err)
			}
			if c.pendingWrite != p || e.Deadline() != idle {
				t.Fatal("pending write lost")
			}
			if err := e.HandleTimeout(idle); !errors.Is(err, ErrIdleTimeout) {
				t.Fatal("idle timeout suppressed", err)
			}
			if !c.closed || c.pendingWrite != nil || !p.done || p.out != nil {
				t.Fatal("idle shutdown retained pending packet")
			}
			request.complete(0, syscall.EMSGSIZE)
			if _, _, ready := p.poll(); ready {
				t.Fatal("late result applied after shutdown")
			}
		})
	}
}

func TestForeignWriteDuplicateBeginReleasesTicket(t *testing.T) {
	p, w, _ := pendingWriteFixture(t)
	first := p.out.Conn.pendingWrite
	ticket, err := w.reserve(p.route)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := beginForeignWrite(ticket, p.out, nil); !errors.Is(err, errPathBinding) {
		t.Fatal(err)
	}
	if p.out.Conn.pendingWrite != first || len(w.reserved) != 0 || len(w.writes) != 1 {
		t.Fatal("duplicate begin replaced owner or leaked ticket")
	}
}

func TestForeignWriteSchedulerOtherConnection(t *testing.T) {
	e, c, peer, out := ownedOutboundFixture(t, wire.Version1, "base-stream")
	p, request := scheduleForeignWrite(t, out)
	e.router.capacity = 2 // The client endpoint fixture normally admits one connection.
	if err := e.router.Add(peer, out.Peer); err != nil {
		t.Fatal(err)
	}
	e.conns = append(e.conns, peer)
	id, err := peer.OpenStream(false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := peer.WriteStream(id, []byte("other connection"), true); err != nil {
		t.Fatal(err)
	}
	next, err := e.NextDatagram(time.Unix(102, 0))
	if err != nil || next == nil || next.Conn != peer || c.pendingWrite != p {
		t.Fatal("pending connection blocked another", err)
	}
	request.complete(len(request.data), nil)
}

func TestForeignWriteTerminalTransition(t *testing.T) {
	for _, mode := range []string{"close", "closing", "draining", "remove"} {
		t.Run(mode, func(t *testing.T) {
			e, c, _, out := ownedOutboundFixture(t, wire.Version1, "base-stream")
			p, request := scheduleForeignWrite(t, out)
			now := time.Unix(102, 0)
			switch mode {
			case "close":
				c.Close()
			case "closing":
				if err := c.CloseWithError(0, "done", now); err != nil {
					t.Fatal(err)
				}
			case "draining":
				c.beginDraining(ErrPeerClosed, now)
			case "remove":
				e.Remove(c)
			}
			if c.pendingWrite != nil || !p.done || p.out != nil {
				t.Fatal("terminal transition retained write")
			}
			request.complete(0, syscall.EMSGSIZE)
			if _, _, ready := p.poll(); ready {
				t.Fatal("terminal completion applied")
			}
			if mode == "closing" || mode == "draining" {
				deadline := e.Deadline()
				if deadline.IsZero() || !deadline.After(now) {
					t.Fatal("lost terminal deadline")
				}
				e.HandleTimeout(deadline)
				if !c.closed {
					t.Fatal("terminal expiry suppressed")
				}
			}
		})
	}
}
