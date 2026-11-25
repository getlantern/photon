package transport

import (
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/packet"
	"github.com/getlantern/photon/internal/wire"
)

func phasePacket(t *testing.T, c *Conn, keys *packet.Keys, phase bool, pn uint64) []byte {
	t.Helper()
	first := byte(0x43)
	if phase {
		first |= 4
	}
	header := append([]byte{first}, c.config.LocalCID...)
	header = binary.BigEndian.AppendUint32(header, uint32(pn))
	b, err := keys.Seal(header, []byte{1}, pn)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func TestPeerKeyUpdate(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(v), func(t *testing.T) {
			client, server := testPair(t, v)
			drivePair(t, client, server, false, false)
			old := server.writeKeys(applicationSpace)
			next, _ := old.Next()
			second, _ := next.Next()
			now := time.Unix(200, 0)
			open := func(keys *packet.Keys, phase bool, pn uint64, at time.Time) error {
				raw := phasePacket(t, client, keys, phase, pn)
				_, _, _, err := client.openApplication(raw, 1+len(client.config.LocalCID), 0, at)
				return err
			}
			if err := open(old, false, 1000, now); err != nil {
				t.Fatal(err)
			}
			if err := open(next, true, 1002, now); err != nil {
				t.Fatal(err)
			}
			if !client.applicationKeys.phase || client.writeKeys(applicationSpace) == client.tls.WriteKeys(levels[applicationSpace]) {
				t.Fatal("sending keys did not advance")
			}
			if err := open(old, false, 1001, now); err != nil {
				t.Fatal("reordered old packet rejected", err)
			}
			if err := open(old, false, 1003, now); !errors.Is(err, ErrKeyUpdate) {
				t.Fatal("old keys with higher PN", err)
			}
			if err := open(second, false, 1004, now); !errors.Is(err, ErrKeyUpdate) {
				t.Fatal("second update without ACK", err)
			}
			client.noteKeyPhaseACK(wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 1002, Largest: 1002}}})
			if err := open(second, false, 1004, now); err != nil {
				t.Fatal(err)
			}
			if client.applicationKeys.phase {
				t.Fatal("second update phase did not wrap")
			}
			if err := open(next, true, 1003, now); err != nil {
				t.Fatal(err)
			}
			expiry := client.applicationKeys.previousExpiry
			if client.RecoveryDeadline() != expiry {
				t.Fatal("old-key retirement deadline missing")
			}
			if err := client.HandleTimeout(expiry); err != nil {
				t.Fatal(err)
			}
			if client.applicationKeys.previous != nil {
				t.Fatal("timer retained old keys")
			}
			if err := open(next, true, 1003, expiry); err == nil {
				t.Fatal("expired old key accepted")
			}
		})
	}
}
func TestKeyUpdateAuthenticatesBeforeTransition(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	keys := server.writeKeys(applicationSpace)
	next, _ := keys.Next()
	now := time.Unix(200, 0)
	raw := phasePacket(t, client, next, true, 1000)
	raw[len(raw)-1] ^= 1
	if _, _, _, err := client.openApplication(raw, 1+len(client.config.LocalCID), 0, now); err == nil {
		t.Fatal("forgery accepted")
	}
	if client.applicationKeys.phase {
		t.Fatal("forgery updated keys")
	}
	raw = phasePacket(t, client, keys, true, 1000)
	if _, _, _, err := client.openApplication(raw, 1+len(client.config.LocalCID), 0, now); !errors.Is(err, ErrKeyUpdate) {
		t.Fatal("wrong authenticated key phase", err)
	}
	client.confirmed = false
	raw = phasePacket(t, client, next, true, 1000)
	if _, _, _, err := client.openApplication(raw, 1+len(client.config.LocalCID), 0, now); !errors.Is(err, ErrKeyUpdate) {
		t.Fatal("update before confirmation", err)
	}
}

func TestLocalKeyUpdates(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(v), func(t *testing.T) {
			client, server := testPair(t, v)
			if err := client.InitiateKeyUpdate(); !errors.Is(err, ErrKeyUpdate) {
				t.Fatal("update before confirmation", err)
			}
			drivePair(t, client, server, false, false)
			for i := 0; i < 4; i++ {
				if err := client.InitiateKeyUpdate(); err != nil {
					t.Fatal(i, err)
				}
				if err := client.InitiateKeyUpdate(); !errors.Is(err, ErrKeyUpdate) {
					t.Fatal("update without peer response", err)
				}
				drivePair(t, client, server, true, true)
				if client.applicationKeys.phase != server.applicationKeys.phase || client.applicationKeys.awaitingRead || !client.applicationKeys.writeAcknowledged {
					t.Fatal("update did not converge")
				}
			}
		})
	}
}
func TestLocalUpdateRetainsOldReadKeysUntilResponse(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	old := server.writeKeys(applicationSpace)
	if err := client.InitiateKeyUpdate(); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(10000, 0)
	client.expirePreviousKeys(now)
	if client.applicationKeys.previous == nil {
		t.Fatal("old keys discarded before response")
	}
	raw := phasePacket(t, client, old, false, 1000)
	if _, _, _, err := client.openApplication(raw, 1+len(client.config.LocalCID), 0, now); err != nil {
		t.Fatal("old peer packet while awaiting response", err)
	}
	next, _ := old.Next()
	raw = phasePacket(t, client, next, true, 1001)
	if _, _, _, err := client.openApplication(raw, 1+len(client.config.LocalCID), 0, now); err != nil {
		t.Fatal(err)
	}
	if client.applicationKeys.awaitingRead || !client.applicationKeys.previousExpiry.After(now) {
		t.Fatal("old-key retention did not start at peer response")
	}
}
func TestSimultaneousKeyUpdates(t *testing.T) {
	client, server := testPair(t, wire.Version2)
	drivePair(t, client, server, false, false)
	if err := client.InitiateKeyUpdate(); err != nil {
		t.Fatal(err)
	}
	if err := server.InitiateKeyUpdate(); err != nil {
		t.Fatal(err)
	}
	drivePair(t, client, server, true, true)
	if !client.applicationKeys.phase || !server.applicationKeys.phase || client.applicationKeys.awaitingRead || server.applicationKeys.awaitingRead {
		t.Fatal("simultaneous updates did not converge")
	}
}
