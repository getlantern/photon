package transport

import (
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/packet"
	"github.com/getlantern/photon/internal/wire"
)

func pathPhasePacket(t *testing.T, c *Conn, keys *packet.Keys, pathID uint32, phase bool, pn uint64) []byte {
	t.Helper()
	first := byte(0x43)
	if phase {
		first |= 4
	}
	header := append([]byte{first}, c.config.LocalCID...)
	header = binary.BigEndian.AppendUint32(header, uint32(pn))
	b, err := keys.SealPath(header, []byte{1}, pn, pathID)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestMultipathKeyPhaseNumbers(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := testPair(t, v)
		drivePair(t, client, server, false, false)
		attachSlowPath(t, client)
		if _, err := client.multipath.issue(2); err != nil {
			t.Fatal(err)
		}
		if _, err := client.multipath.application(2); err != nil {
			t.Fatal(err)
		}
		old := server.writeKeys(applicationSpace)
		next, _ := old.Next()
		second, _ := next.Next()
		now := time.Unix(101, 0)
		open := func(id uint32, keys *packet.Keys, phase bool, pn uint64) error {
			b := pathPhasePacket(t, client, keys, id, phase, pn)
			_, _, _, err := client.openApplicationPath(id, b, 1+len(client.config.LocalCID), pn, now)
			return err
		}
		for id, pn := range []uint64{1000, 5, 5000} {
			if err := open(uint32(id), old, false, pn); err != nil {
				t.Fatal(err)
			}
		}
		// A lower packet number on path 1 can trigger the connection-wide update.
		if err := open(1, next, true, 6); err != nil {
			t.Fatal("cross-path number comparison", err)
		}
		if !client.applicationKeys.phase {
			t.Fatal("key update not shared")
		}
		if err := open(0, old, false, 1001); err != nil {
			t.Fatal("delayed old-key path-zero packet", err)
		}
		if err := open(2, old, false, 5001); err != nil {
			t.Fatal("delayed old-key secondary packet", err)
		}
		if err := open(0, next, true, 1001); !errors.Is(err, ErrKeyUpdate) {
			t.Fatal("current key reused old-phase number", err)
		}
		if err := open(0, next, true, 1003); err != nil {
			t.Fatal(err)
		}
		if err := open(0, old, false, 1002); err != nil {
			t.Fatal("reordered old packet", err)
		}
		// The old-packet high-water mark must include late old packets too.
		if err := open(0, next, true, 1002); !errors.Is(err, ErrKeyUpdate) {
			t.Fatal("late old packet crossed phase boundary", err)
		}
		if err := open(0, old, false, 1004); !errors.Is(err, ErrKeyUpdate) {
			t.Fatal("old packet above new phase", err)
		}
		if err := open(2, next, true, 5002); err != nil {
			t.Fatal(err)
		}
		if err := open(1, second, false, 7); !errors.Is(err, ErrKeyUpdate) {
			t.Fatal("updated again without ACK", err)
		}
		client.noteKeyPhaseACK(wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 6, Largest: 6}}}})
		if !client.applicationKeys.ackSent {
			t.Fatal("PATH_ACK did not confirm receive phase")
		}
		if err := open(0, second, false, 1004); err != nil {
			t.Fatal("second update compared another path", err)
		}
		if err := open(1, next, true, 7); err != nil {
			t.Fatal("previous-generation packet on lagging path", err)
		}
		if err := open(1, second, false, 8); err != nil {
			t.Fatal(err)
		}
		// Unknown path input must not grow key state or trigger key advancement.
		if err := open(3, second, false, 0); !errors.Is(err, packet.ErrPacket) {
			t.Fatal("unallocated path accepted", err)
		}
	}
}

func TestMultipathLocalKeyUpdateReordering(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	attachSlowPath(t, client)
	old := server.writeKeys(applicationSpace)
	next, _ := old.Next()
	now := time.Unix(101, 0)
	open := func(id uint32, keys *packet.Keys, phase bool, pn uint64) error {
		b := pathPhasePacket(t, client, keys, id, phase, pn)
		_, _, _, err := client.openApplicationPath(id, b, 1+len(client.config.LocalCID), pn, now)
		return err
	}
	if err := open(1, old, false, 100); err != nil {
		t.Fatal(err)
	}
	if err := client.InitiateKeyUpdate(); err != nil {
		t.Fatal(err)
	}
	if err := open(0, next, true, 1000); err != nil {
		t.Fatal(err)
	}
	if client.applicationKeys.awaitingRead {
		t.Fatal("global response gate not cleared")
	}
	if err := open(1, old, false, 101); err != nil {
		t.Fatal("old key on path not yet switched", err)
	}
	client.noteKeyPhaseACK(wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 101, Largest: 101}}}})
	if client.applicationKeys.ackSent {
		t.Fatal("old-phase PATH_ACK confirmed new phase")
	}
	if err := open(1, next, true, 102); err != nil {
		t.Fatal(err)
	}
	client.noteKeyPhaseACK(wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 102, Largest: 102}}}})
	if !client.applicationKeys.ackSent {
		t.Fatal("new-phase PATH_ACK ignored")
	}
}

func TestMultipathWritePhaseAcknowledgment(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	path := attachSlowPath(t, client)
	client.applicationKeys.firstSent = 10
	client.applicationKeys.writeAcknowledged = false
	path.keyNumbers.firstSent = 100
	frame := wire.PathAckFrame{PathID: 1, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 99, Largest: 99}}}}
	client.noteKeyPhaseWriteACK(frame, time.Unix(101, 0))
	if client.applicationKeys.writeAcknowledged {
		t.Fatal("used path-zero threshold")
	}
	frame.Ranges[0] = wire.AckRange{Smallest: 100, Largest: 100}
	client.noteKeyPhaseWriteACK(frame, time.Unix(101, 0))
	if !client.applicationKeys.writeAcknowledged {
		t.Fatal("path acknowledgment not connection-wide")
	}
}
