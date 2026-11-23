package transport

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/packet"
	"github.com/getlantern/photon/internal/wire"
)

func pathDecodeFixture(t *testing.T, v wire.Version) (*Conn, *packet.Keys, []byte) {
	t.Helper()
	client, server := testPair(t, v)
	drivePair(t, client, server, false, false)
	attachSlowPath(t, client)
	state := client.multipath.paths[1]
	var cid []byte
	for seq, id := range state.local.active {
		if seq == 0 {
			cid = bytes.Clone(id.cid)
			client.multipath.markSent(1, seq)
		}
	}
	return client, server.writeKeys(applicationSpace), cid
}
func sealPathPayload(t *testing.T, keys *packet.Keys, cid []byte, id uint32, pn uint64, payload []byte) []byte {
	t.Helper()
	h := append([]byte{0x43}, cid...)
	h = binary.BigEndian.AppendUint32(h, uint32(pn))
	raw, err := keys.SealPath(h, payload, pn, id)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestPathPacketCIDRoutingAndReplay(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		c, keys, cid := pathDecodeFixture(t, v)
		now := time.Unix(101, 0)
		for _, id := range []uint32{0, 1} {
			dest := cid
			if id == 0 {
				dest = c.config.LocalCID
			}
			raw := sealPathPayload(t, keys, dest, id, 100, []byte{1, 0x10, 20})
			out, err := c.decodeMultipathPacket(raw, now)
			if err != nil || out == nil || out.pathID != uint64(id) || out.number != 100 || out.receipt != NewPacket || len(out.frames) != 2 {
				t.Fatalf("path %d: %#v %v", id, out, err)
			}
			duplicate, err := c.decodeMultipathPacket(raw, now)
			if err != nil || duplicate == nil || duplicate.receipt != DuplicatePacket || len(duplicate.frames) != 0 {
				t.Fatal("duplicate delivered frames", err)
			}
			if !c.multipath.paths[uint64(id)].application.space.ackPending {
				t.Fatal("ACK not scheduled on source path")
			}
		}
		// Authentication binds the packet to the CID-selected path, not an external
		// caller-supplied ID. A path-1 nonce with path-zero CID must fail.
		bad := sealPathPayload(t, keys, c.config.LocalCID, 1, 101, []byte{1})
		count := c.failedAuth
		if out, err := c.decodeMultipathPacket(bad, now); err != nil || out != nil || c.failedAuth != count+1 {
			t.Fatal("wrong-path nonce accepted", err)
		}
		if c.multipath.paths[0].application.space.received.Expected() != 101 {
			t.Fatal("failed authentication advanced receive PN")
		}
	}
}

func TestPathPacketDoesNotAllocateOnInput(t *testing.T) {
	c, keys, cid := pathDecodeFixture(t, wire.Version1)
	now := time.Unix(101, 0)
	initialPaths, auth := len(c.multipath.paths), c.failedAuth
	unknown := sealPathPayload(t, keys, []byte("unknown0"), 2, 0, []byte{1})
	if out, err := c.decodeMultipathPacket(unknown, now); err != nil || out != nil {
		t.Fatal(out, err)
	}
	frames, err := c.multipath.issue(2)
	if err != nil {
		t.Fatal(err)
	}
	raw := sealPathPayload(t, keys, frames[0].ConnectionID, 2, 0, []byte{1})
	if out, err := c.decodeMultipathPacket(raw, now); err != nil || out != nil {
		t.Fatal("unsent CID routed", err)
	}
	c.multipath.markSent(2, frames[0].Sequence)
	if out, err := c.decodeMultipathPacket(raw, now); err != nil || out != nil || c.multipath.paths[2].application != nil {
		t.Fatal("packet allocated application path", err)
	}
	if len(c.multipath.paths) != initialPaths+1 || c.failedAuth != auth {
		t.Fatal("unroutable packet changed state")
	}
	raw = sealPathPayload(t, keys, cid, 1, 0, []byte{1})
	for n := 0; n < len(raw); n++ {
		if out, err := c.decodeMultipathPacket(raw[:n], now); err != nil || out != nil {
			t.Fatal("truncated packet", n, err)
		}
	}
}

func TestPathPacketValidatesBeforeRecording(t *testing.T) {
	for _, payload := range [][]byte{nil, {1, 0x21}, {1, 0x06, 0}} {
		c, keys, cid := pathDecodeFixture(t, wire.Version2)
		raw := sealPathPayload(t, keys, cid, 1, 99, payload)
		if out, err := c.decodeMultipathPacket(raw, time.Unix(101, 0)); err == nil || out != nil {
			t.Fatal("invalid payload accepted")
		}
		if c.multipath.paths[1].application.space.received.Expected() != 0 {
			t.Fatal("invalid frames committed receive state")
		}
	}
}

func TestPathPacketIntegrityLimitShared(t *testing.T) {
	c, keys, cid := pathDecodeFixture(t, wire.Version1)
	c.integrityLimit = 2
	now := time.Unix(101, 0)
	for i, dest := range [][]byte{c.config.LocalCID, cid} {
		raw := sealPathPayload(t, keys, dest, uint32(i), 100, []byte{1})
		raw[len(raw)-1] ^= 1
		out, err := c.decodeMultipathPacket(raw, now)
		if out != nil {
			t.Fatal("unauthenticated packet returned")
		}
		if i == 0 && err != nil {
			t.Fatal(err)
		}
		if i == 1 {
			requirePathError(t, err, 0xf, 0)
		}
	}
	if c.failedAuth != 2 {
		t.Fatal("integrity failures not shared")
	}
}
