package transport

import (
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

func TestLargeDatagramWriteRejection(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, discovery := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/discovery=%t", version, discovery), func(t *testing.T) {
				e, c, peer := mtuWriteEndpointPair(t, version, 1500)
				defer e.Close()
				c.peerParams.MaxDatagramFrameSize = 65535
				c.config.InitialPacketSize = 1452
				c.config.enablePathMTUDiscovery = true
				c.config.DisablePathMTUDiscovery = !discovery
				app := c.multipath.paths[1].application
				c.seedInitialPathMTU(app)
				if err := c.SendDatagram(make([]byte, 1410)); err != nil {
					t.Fatal(err)
				}
				now := time.Unix(101, 0)
				idle, window := c.idle, app.congestion.Window()
				out, err := e.NextDatagram(now)
				if err != nil || out == nil || out.datagram == nil || out.mtu != nil || out.large != nil || len(out.Data) <= 1200 {
					t.Fatal("large DATAGRAM classification", err)
				}
				pn, sealed := out.datagram.number, c.sealed[applicationSpace]
				if c.pendingDatagrams != 0 || c.queuedBytes != 0 {
					t.Fatal("sent datagram retained payload accounting")
				}
				e.HandleWriteError(out, fmt.Errorf("socket: %w", syscall.EMSGSIZE), now)
				if c.closed || len(e.conns) != 1 || app.sent.BytesInFlight != 0 || app.congestion.Window() != window || c.idle != idle {
					t.Fatal("write rejection accounting")
				}
				if app.space.nextPN != pn+1 || c.sealed[applicationSpace] != sealed || len(app.space.queue) != 0 || len(app.space.sent) != 0 || c.pendingDatagrams != 0 || c.queuedBytes != 0 {
					t.Fatal("DATAGRAM rejection retransmitted or rewound state")
				}
				if app.packetPayloadLimit() != 1200 || !app.mtuNeedsValidation || app.mtuRaiseDeadline().IsZero() == discovery {
					t.Fatal("fallback/discovery state")
				}
				if _, err := app.sent.OnACK(recovery.Application, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: pn, Largest: pn}}}, now, 0, 0, true); !errors.Is(err, recovery.ErrACK) {
					t.Fatal("ACK accepted for locally rejected packet", err)
				}
				out, err = e.NextDatagram(now.Add(time.Second))
				if err != nil || out == nil || len(out.Data) != 1200 || out.datagram != nil || app.validation.state != PathValidating {
					t.Fatal("baseline validation missing or misclassified", err)
				}
				peer.config.Parameters.MaxDatagramFrameSize = 65535
				remote := peer.multipath.paths[1].application
				remote.budget.Validate()
				remote.validation.state = PathValidated
				now = now.Add(time.Second)
				if err := peer.handleMultipathPacket(out.Data, now); err != nil {
					t.Fatal(err)
				}
				// The smaller datagram waits behind baseline validation, then
				// completes over protected packets after the path response.
				if err := c.SendDatagram([]byte("after fallback")); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 20 && len(peer.datagrams.queue) == 0; i++ {
					now = now.Add(time.Millisecond)
					response, err := peer.nextPacketOnPath(1, now)
					if err != nil {
						t.Fatal(err)
					}
					if response != nil {
						if err := c.handleMultipathPacket(response, now); err != nil {
							t.Fatal(err)
						}
					}
					next, err := e.NextDatagram(now)
					if err != nil {
						t.Fatal(err)
					}
					if next != nil {
						if err := peer.handleMultipathPacket(next.Data, now); err != nil {
							t.Fatal(err)
						}
					}
				}
				got, err := peer.ReceiveDatagram()
				if err != nil || string(got) != "after fallback" || len(peer.datagrams.queue) != 0 {
					t.Fatal("post-fallback delivery", string(got), err)
				}
			})
		}
	}
}

func TestDatagramWriteErrorKeepsFatalCases(t *testing.T) {
	for _, tc := range []struct {
		size int
		err  error
	}{{100, syscall.EMSGSIZE}, {1410, syscall.ECONNREFUSED}} {
		e, c := mtuWriteEndpoint(t, wire.Version1)
		defer e.Close()
		c.peerParams.MaxDatagramFrameSize = 65535
		c.config.InitialPacketSize = 1452
		c.config.DisablePathMTUDiscovery = true
		c.seedInitialPathMTU(c.multipath.paths[1].application)
		if err := c.SendDatagram(make([]byte, tc.size)); err != nil {
			t.Fatal(err)
		}
		out, err := e.NextDatagram(time.Unix(101, 0))
		if err != nil || out == nil {
			t.Fatal(err)
		}
		e.HandleWriteError(out, tc.err, time.Unix(101, 0))
		if !c.closed {
			t.Fatal("unrecoverable write failure ignored", tc)
		}
	}
}
