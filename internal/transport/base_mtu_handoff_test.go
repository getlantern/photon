package transport

import (
	"fmt"
	"net/netip"
	"syscall"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestBaseMTUFallbackHandoff(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, sent := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/sent=%t", v, sent), func(t *testing.T) {
				c, peer := testPair(t, v, streamParameters())
				c.config.InitialPacketSize = 1452
				drivePair(t, c, peer, false, false)
				e, err := NewClientEndpoint(c, netip.MustParseAddrPort("127.0.0.1:443"))
				if err != nil {
					t.Fatal(err)
				}
				defer e.Close()
				id, err := c.OpenStream(false)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := c.WriteStream(id, make([]byte, 1252), false); err != nil {
					t.Fatal(err)
				}
				now := time.Unix(101, 0)
				out, err := e.NextDatagram(now)
				if err != nil || out == nil {
					t.Fatal(err)
				}
				e.HandleWriteError(out, syscall.EMSGSIZE, now)
				if sent {
					out, err = e.NextDatagram(now.Add(time.Millisecond))
					if err != nil || out == nil {
						t.Fatal(err)
					}
					if err := peer.HandleDatagram(out.Data, now.Add(time.Millisecond)); err != nil {
						t.Fatal(err)
					}
				}
				challenge, validation, pn, charge := c.baseMTUChallenge, c.validation, c.spaces[applicationSpace].nextPN, c.queuedBytes
				p, err := adoptPathZero(c, 3, 3, 4)
				if err != nil {
					t.Fatal(err)
				}
				p.baseOnly = true
				c.multipath = p
				app := p.paths[0].application
				if app.packetPayloadLimit() != 1200 {
					t.Fatal("handoff restored failed packet size", app.packetPayloadLimit())
				}
				if !app.mtuNeedsValidation || app.mtuChallenge != challenge || app.validation.state != validation.state || app.validation.expires != validation.expires || app.space.nextPN != pn || c.queuedBytes != charge {
					t.Fatal("handoff lost fallback validation state")
				}
				if c.baseMTUChallenge != nil || c.baseMTUNeedsValidation {
					t.Fatal("base retained validation ownership")
				}
				if !sent {
					b, err := c.nextPacketOnPath(0, now.Add(2*time.Millisecond))
					if err != nil || len(b) != 1200 || app.mtuChallenge != nil {
						t.Fatal("reserved challenge lost during handoff", len(b), err)
					}
					if err := peer.HandleDatagram(b, now.Add(2*time.Millisecond)); err != nil {
						t.Fatal(err)
					}
				}
				for i := 0; i < 20; i++ {
					at := now.Add(time.Duration(i+3) * time.Millisecond)
					b, err := peer.NextDatagram(at)
					if err != nil {
						t.Fatal(err)
					}
					if b != nil {
						if err := c.handleMultipathPacket(b, at); err != nil {
							t.Fatal(err)
						}
					}
					b, err = c.nextPacketOnPath(0, at)
					if err != nil {
						t.Fatal(err)
					}
					if len(b) > 1200 {
						t.Fatal("handoff emitted failed size")
					}
					if b != nil {
						if err := peer.HandleDatagram(b, at); err != nil {
							t.Fatal(err)
						}
					}
				}
				if app.mtuNeedsValidation || app.validation.state != PathValidated {
					t.Fatal("handoff lost response matching")
				}
				buf := make([]byte, 1252)
				if n, err := peer.ReadStream(id, buf); n != len(buf) || err != nil {
					t.Fatal("queued stream lost at handoff", n, err)
				}
			})
		}
	}
}

func TestCompletedBaseMTUFallbackHandoff(t *testing.T) {
	for _, discovery := range []bool{false, true} {
		c, peer := testPair(t, wire.Version1)
		drivePair(t, c, peer, false, false)
		c.config.InitialPacketSize = 1452
		c.config.enablePathMTUDiscovery = discovery
		c.peerParams.MaxUDPPayloadSize = 1400
		c.basePacketLimit = 1200
		c.baseMTUFallbackAt = time.Unix(101, 0)
		c.baseMTUNeedsValidation = true // cleared by the next output operation normally
		c.validation = pathValidation{state: PathValidated}
		p, err := adoptPathZero(c, 3, 3, 4)
		if err != nil {
			t.Fatal(err)
		}
		app := p.paths[0].application
		if app.mtuNeedsValidation || app.packetPayloadLimit() != 1200 || app.mtu.ceiling != 1400 {
			t.Fatal("completed fallback state lost")
		}
		if _, ok := app.mtu.next(); ok {
			t.Fatal("handoff reopened discovery immediately")
		}
		if discovery {
			at := c.baseMTUFallbackAt.Add(10 * time.Minute)
			if app.mtuRaiseDeadline() != at || !app.expireMTURaise(at) {
				t.Fatal("fallback cooldown lost")
			}
			size, ok := app.mtu.next()
			if !ok || size <= 1200 || size > 1400 || app.packetPayloadLimit() != 1200 {
				t.Fatal("raise skipped fresh probe confirmation")
			}
		} else if !app.mtuRaiseDeadline().IsZero() {
			t.Fatal("disabled discovery acquired timer")
		}
	}
}
