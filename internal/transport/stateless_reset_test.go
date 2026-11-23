package transport

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func resetDatagram(token [16]byte) []byte {
	return append([]byte{0x40, 0xaa, 0xbb, 0xcc, 0xdd}, token[:]...)
}
func TestStatelessResetOnlyUsedActiveCID(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(v), func(t *testing.T) {
			client, server := testPair(t, v)
			drivePair(t, client, server, false, false)
			now := time.Unix(200, 0)
			old := client.peerIDs.active[client.peerIDs.current]
			old.hasToken = true
			old.token = [16]byte{1, 2, 3}
			client.peerIDs.active[client.peerIDs.current] = old
			if !old.used {
				t.Fatal("sent peer CID not marked used")
			}
			if err := client.RotatePeerID(); err != nil {
				t.Fatal(err)
			}
			current := client.peerIDs.active[client.peerIDs.current]
			if current.used {
				t.Fatal("unsent CID marked used")
			}
			for _, token := range [][16]byte{old.token, current.token} {
				if err := client.HandleDatagram(resetDatagram(token), now); err != nil {
					t.Fatal("unused/retired token accepted", err)
				}
			}
			if b, err := client.NextDatagram(now); err != nil || len(b) == 0 {
				t.Fatal(err)
			}
			if !client.peerIDs.active[client.peerIDs.current].used {
				t.Fatal("new destination CID not marked used")
			}
			if err := client.HandleDatagram(resetDatagram(current.token), now); !errors.Is(err, ErrStatelessReset) {
				t.Fatal("reset not detected", err)
			} else {
				var reset *StatelessResetError
				var network net.Error
				if !errors.As(err, &reset) || !errors.As(err, &network) || network.Timeout() || !network.Temporary() || !errors.Is(err, net.ErrClosed) {
					t.Fatal("reset error contract", err)
				}
			}
			if b, err := client.NextDatagram(now); len(b) != 0 || !errors.Is(err, ErrStatelessReset) {
				t.Fatal("sent after reset", err)
			}
			if !client.Draining() || !client.RecoveryDeadline().After(now) {
				t.Fatal("reset missing draining deadline")
			}
		})
	}
}
func TestAuthenticatedPacketNotStatelessReset(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	raw := makePeerPacket(t, server, applicationSpace, 1000, wire.PingFrame{})
	id := client.peerIDs.active[client.peerIDs.current]
	copy(id.token[:], raw[len(raw)-16:])
	id.hasToken = true
	client.peerIDs.active[client.peerIDs.current] = id
	if err := client.HandleDatagram(raw, time.Unix(200, 0)); err != nil {
		t.Fatal("authenticated packet mistaken for reset", err)
	}
	damaged := bytes.Clone(raw)
	damaged[1] ^= 0xff // Unknown destination CID; reset suffix remains intact.
	if err := client.HandleDatagram(damaged, time.Unix(200, 0)); !errors.Is(err, ErrStatelessReset) {
		t.Fatal("unknown CID reset not detected", err)
	}
}
func TestResetTokenLengthAndDuplicateCID(t *testing.T) {
	p := newPeerIDs([]byte("original"), 3)
	f := wire.NewConnectionIDFrame{Sequence: 1, ConnectionID: []byte("peer-id1"), StatelessResetToken: [16]byte{9}}
	if _, err := p.add(f); err != nil {
		t.Fatal(err)
	}
	id := p.active[1]
	id.used = true
	p.active[1] = id
	if _, err := p.add(f); err != nil || !p.active[1].used {
		t.Fatal("duplicate CID lost use tracking", err)
	}
	c := &Conn{peerIDs: p}
	if c.isStatelessReset(f.StatelessResetToken[:]) {
		t.Fatal("short datagram reset")
	}
	if !c.isStatelessReset(resetDatagram(f.StatelessResetToken)) {
		t.Fatal("active token mismatch")
	}
	bad := resetDatagram(f.StatelessResetToken)
	bad[len(bad)-1] ^= 1
	if c.isStatelessReset(bad) {
		t.Fatal("partial token matched")
	}
}
