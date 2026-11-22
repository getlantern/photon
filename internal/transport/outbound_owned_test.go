package transport

import (
	"bytes"
	"fmt"
	"net/netip"
	"syscall"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func ownedOutboundFixture(t *testing.T, v wire.Version, kind string) (*ServerEndpoint, *Conn, *Conn, *Outbound) {
	t.Helper()
	var e *ServerEndpoint
	var c, peer *Conn
	if kind == "base-stream" || kind == "base-datagram" {
		params := streamParameters()
		params.MaxDatagramFrameSize = 65535
		c, peer = testPair(t, v, params)
		c.config.InitialPacketSize = 1452
		drivePair(t, c, peer, false, false)
		var err error
		e, err = NewClientEndpoint(c, netip.MustParseAddrPort("127.0.0.1:443"))
		if err != nil {
			t.Fatal(err)
		}
	} else {
		e, c, peer = mtuWriteEndpointPair(t, v, 1500)
	}
	t.Cleanup(e.Close)
	switch kind {
	case "stream":
		c.multipath.paths[1].application.mtu.confirmed = 3000
		c.multipath.paths[1].application.mtu.upper = 3000
	case "datagram":
		c.peerParams.MaxDatagramFrameSize = 65535
		c.config.InitialPacketSize = 1452
		c.config.enablePathMTUDiscovery = true
		c.seedInitialPathMTU(c.multipath.paths[1].application)
	}
	if kind == "stream" || kind == "base-stream" {
		id, err := c.OpenStream(false)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.WriteStream(id, make([]byte, 1252), true); err != nil {
			t.Fatal(err)
		}
	} else if kind == "datagram" || kind == "base-datagram" {
		if err := c.SendDatagram(make([]byte, 1252)); err != nil {
			t.Fatal(err)
		}
	}
	var descriptor Outbound
	var metadata outboundBaseLarge
	var storage [4096]byte
	out, err := e.nextDatagramInto(time.Unix(101, 0), &descriptor, storage[:], &metadata)
	if err != nil || out == nil {
		t.Fatal("missing packet", err)
	}
	switch kind {
	case "probe":
		if out.mtu == nil {
			t.Fatal("missing probe metadata")
		}
	case "stream":
		if out.large == nil {
			t.Fatal("missing stream metadata")
		}
	case "datagram":
		if out.datagram == nil {
			t.Fatal("missing datagram metadata")
		}
	default:
		if out.baseLarge != &metadata {
			t.Fatal("base metadata not borrowed")
		}
	}
	return e, c, peer, out
}

func TestOwnedOutboundSurvivesBufferReuse(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, kind := range []string{"probe", "stream", "datagram", "base-stream", "base-datagram"} {
			t.Run(fmt.Sprintf("%x/%s", uint32(v), kind), func(t *testing.T) {
				e, c, _, out := ownedOutboundFixture(t, v, kind)
				original := append([]byte(nil), out.Data...)
				retained := out.retain()
				if retained.Conn != c || retained.ecn != out.ecn {
					t.Fatal("lost owning engine identity")
				}
				clear(out.Data)
				if out.mtu != nil {
					*out.mtu = outboundMTUProbe{}
				}
				if out.large != nil {
					*out.large = outboundLargeStream{}
				}
				if out.datagram != nil {
					*out.datagram = outboundMTUProbe{}
				}
				if out.baseLarge != nil {
					*out.baseLarge = outboundBaseLarge{}
				}
				*out = Outbound{}
				if !bytes.Equal(retained.Data, original) {
					t.Fatal("retained wire bytes changed")
				}
				e.HandleWriteError(retained, syscall.EMSGSIZE, time.Unix(102, 0))
				if c.closed || c.Err() != nil {
					t.Fatal("borrowed metadata reuse broke write rejection")
				}
				if c.multipath == nil {
					if c.recovery.BytesInFlight != 0 || !c.baseMTUNeedsValidation {
						t.Fatal("base rejection not applied")
					}
				} else if c.multipath.paths[1].application.sent.BytesInFlight != 0 {
					t.Fatal("path rejection not applied")
				}
			})
		}
	}
}

func TestDelayedWriteRejectionPreservesReceivedActivity(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, kind := range []string{"probe", "stream", "datagram", "base-stream", "base-datagram"} {
			t.Run(fmt.Sprintf("%x/%s", uint32(v), kind), func(t *testing.T) {
				e, c, peer, out := ownedOutboundFixture(t, v, kind)
				retained := out.retain()
				now := time.Unix(101, 0) // Same timestamp as packet construction.
				if c.multipath == nil {
					raw := makePeerPacket(t, peer, applicationSpace, 1000, wire.PingFrame{})
					if err := c.HandleDatagram(raw, now); err != nil {
						t.Fatal(err)
					}
				} else {
					payload, _ := (wire.PingFrame{}).Append(nil)
					raw := sealPathPayload(t, peer.writeKeys(applicationSpace), c.multipath.paths[1].local.active[0].cid, 1, 1000, payload)
					if err := c.handleMultipathPacket(raw, now); err != nil {
						t.Fatal(err)
					}
				}
				idle, revision := c.idle, c.idleRevision
				e.HandleWriteError(retained, syscall.EMSGSIZE, now.Add(time.Millisecond))
				if c.closed || c.idle != idle || c.idleRevision != revision {
					t.Fatal("delayed write rejection rewound received activity")
				}
			})
		}
	}
}

func TestIdleRollbackDetectsEqualClockActivity(t *testing.T) {
	c := &Conn{}
	old := time.Unix(100, 0)
	now := old.Add(time.Second)
	c.receivedIdleActivity(old)
	before := c.idle
	c.sentIdleActivity(now, true)
	after, revision := c.idle, c.idleRevision
	c.receivedIdleActivity(now)
	c.sentIdleActivity(now, true)
	if c.idle != after {
		t.Fatal("fixture does not recreate same idle values")
	}
	c.restoreIdleActivity(before, revision)
	if c.idle != after {
		t.Fatal("equal clock concealed intervening activity")
	}
	// An immediate rejection still restores earlier semantic state once.
	current := c.idleRevision
	c.restoreIdleActivity(before, current)
	if c.idle != before || c.idleRevision == current {
		t.Fatal("rollback was not single use")
	}
}
