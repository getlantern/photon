package transport

import (
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestMultipathEncryptedCIDExchangeAndEstablishment(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := testPair(t, version, streamParameters())
		drivePair(t, client, server, false, false)
		// The manually installed owner below models a migration-capable server.
		client.peerParams.DisableActiveMigration = false
		ca := netip.MustParseAddrPort("192.0.2.1:443")
		sa := netip.MustParseAddrPort("192.0.2.2:444")
		for _, c := range []*Conn{client, server} {
			p, err := adoptPathZero(c, 3, 3, 4)
			if err != nil {
				t.Fatal(err)
			}
			c.multipath = p
			local, remote := ca, sa
			if c.config.Server {
				local, remote = sa, ca
			}
			if err := c.bindPreparedPath(0, local, remote); err != nil {
				t.Fatal(err)
			}
			if err := c.advertisePathCIDs(1); err != nil {
				t.Fatal(err)
			}
			for _, cid := range p.paths[1].local.active {
				if _, ok := p.lookupLocal(cid.cid); ok {
					t.Fatal("unsent CID routable")
				}
			}
		}
		now := time.Unix(101, 0)
		exchange := func(rounds int) {
			t.Helper()
			for i := 0; i < rounds; i++ {
				for _, pair := range [][2]*Conn{{client, server}, {server, client}} {
					_, tuple, b, err := pair[0].nextBoundMultipathPacket(now)
					if err != nil {
						t.Fatal(err)
					}
					if b == nil {
						continue
					}
					if err := pair[1].handleBoundMultipathPacket(b, tuple.remote, tuple.local, now); err != nil {
						t.Fatal(err)
					}
				}
				now = now.Add(time.Millisecond)
			}
		}
		exchange(24)
		for _, c := range []*Conn{client, server} {
			s := c.multipath.paths[1]
			if s.application != nil || len(s.peer.active) == 0 {
				t.Fatal("CID exchange did not prepare unused path")
			}
			for _, cid := range s.local.active {
				if id, ok := c.multipath.lookupLocal(cid.cid); !ok || id != 1 {
					t.Fatal("sent CID missing route")
				}
			}
		}
		if err := client.initiateMultipathPath(1, ca, sa); err != nil {
			t.Fatal(err)
		}
		exchange(24)
		for _, c := range []*Conn{client, server} {
			if app := c.multipath.paths[1].application; app == nil || app.validation.state != PathValidated {
				t.Fatal("path establishment failed")
			}
		}
	}
}

func TestMultipathCIDAdvertisementQueueRollback(t *testing.T) {
	c, _ := sendingPathPair(t, wire.Version1)
	before := len(c.multipath.localTemplate.used)
	c.queuedBytes = maxSendBytes
	if err := c.advertisePathCIDs(2); !errors.Is(err, ErrSendBuffer) {
		t.Fatal(err)
	}
	if c.multipath.paths[2] != nil || len(c.multipath.localTemplate.used) != before {
		t.Fatal("failed queue consumed CID state")
	}
	c.queuedBytes = 0
	if err := c.advertisePathCIDs(2); err != nil {
		t.Fatal(err)
	}
	charged := c.queuedBytes
	if charged == 0 {
		t.Fatal("missing queue charge")
	}
	if err := c.advertisePathCIDs(2); err != nil {
		t.Fatal(err)
	}
	if c.queuedBytes != charged {
		t.Fatal("duplicate advertisements")
	}
}
