package transport

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// A large path fails with both transmitted and queued STREAM data. Its smaller
// survivor must retain all bytes, re-packetize, and accept an old-path late ACK.
func TestUnequalMTUPathFailureTransfer(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, lateACK := range []bool{false, true} {
			t.Run(fmt.Sprintf("%x/late-ack=%t", version, lateACK), func(t *testing.T) {
				c, peer := sendingPathPair(t, version)
				for _, conn := range []*Conn{c, peer} {
					for _, id := range []uint64{0, 1} {
						a := conn.multipath.paths[id].application
						a.budget.Validate()
						a.validation.state = PathValidated
					}
				}
				c.multipath.paths[0].backup = true
				a := c.multipath.paths[1].application
				if err := c.startPathMTUSearch(1, 4800); err != nil {
					t.Fatal(err)
				}
				now := time.Unix(101, 0)
				send := func(from, to *Conn, id uint64) []byte {
					t.Helper()
					b, err := from.nextPacketOnPath(id, now)
					if err != nil {
						t.Fatal(err)
					}
					if b != nil {
						if err := to.handleMultipathPacket(b, now); err != nil {
							t.Fatal(err)
						}
					}
					return b
				}
				if b := send(c, peer, 1); len(b) != 3000 {
					t.Fatal("probe", len(b))
				}
				now = now.Add(time.Millisecond)
				send(peer, c, 1)
				if a.mtu.confirmed != 3000 {
					t.Fatal("MTU not confirmed")
				}
				id, err := c.OpenStream(false)
				if err != nil {
					t.Fatal(err)
				}
				payload := make([]byte, 7500)
				for i := range payload {
					payload[i] = byte(i * 37)
				}
				for i := 0; i < 3; i++ {
					if n, err := c.WriteStream(id, payload[i*2500:(i+1)*2500], i == 2); err != nil || n != 2500 {
						t.Fatal("admission", n, err)
					}
				}
				now = now.Add(time.Millisecond)
				original, err := c.nextPacketOnPath(1, now)
				if err != nil || len(original) <= 1200 {
					t.Fatal("large original", len(original), err)
				}
				var delayed []byte
				if lateACK {
					if err := peer.handleMultipathPacket(original, now); err != nil {
						t.Fatal(err)
					}
					now = now.Add(time.Millisecond)
					delayed, err = peer.nextPacketOnPath(1, now)
					if err != nil || delayed == nil {
						t.Fatal("delayed ACK", err)
					}
				}
				charge := c.queuedBytes
				if err := c.abandonFailedPath(1); err != nil {
					t.Fatal(err)
				}
				if len(a.space.queue) != 0 || c.queuedBytes < charge {
					t.Fatal("evacuation discarded data")
				}
				small := c.multipath.paths[0].application
				if small.packetPayloadLimit() != 1200 {
					t.Fatal("survivor MTU")
				}
				for i := 0; i < 300 && c.StreamAcknowledged(id) != nil; i++ {
					now = now.Add(time.Millisecond)
					if b := send(c, peer, 0); len(b) > 1200 {
						t.Fatal("oversized survivor packet", len(b))
					}
					if i == 2 && delayed != nil {
						if err := c.handleMultipathPacket(delayed, now); err != nil {
							t.Fatal("late original ACK", err)
						}
						delayed = nil
					}
					now = now.Add(time.Millisecond)
					send(peer, c, 0)
				}
				if err := c.StreamAcknowledged(id); err != nil {
					t.Fatal("incomplete transfer", err)
				}
				if delayed != nil || c.queuedBytes != 0 || small.sent.BytesInFlight != 0 {
					t.Fatal("completion accounting", c.queuedBytes, small.sent.BytesInFlight)
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

// Both paths stay active. Admission chooses the large path, then the scheduler
// borrows onto the small path while continuing to transmit on the large one.
func TestUnequalMTUConcurrentScheduling(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			c, peer := sendingPathPair(t, version)
			for _, conn := range []*Conn{c, peer} {
				for _, id := range []uint64{0, 1} {
					a := conn.multipath.paths[id].application
					a.budget.Validate()
					a.validation.state = PathValidated
				}
			}
			now := time.Unix(101, 0)
			if err := c.startPathMTUSearch(1, 4800); err != nil {
				t.Fatal(err)
			}
			b, err := c.nextPacketOnPath(1, now)
			if err != nil || len(b) != 3000 {
				t.Fatal("probe", err)
			}
			if err := peer.handleMultipathPacket(b, now); err != nil {
				t.Fatal(err)
			}
			now = now.Add(time.Millisecond)
			b, err = peer.nextPacketOnPath(1, now)
			if err != nil || b == nil {
				t.Fatal("probe ACK", err)
			}
			if err := c.handleMultipathPacket(b, now); err != nil {
				t.Fatal(err)
			}
			if c.multipath.paths[1].application.mtu.confirmed != 3000 {
				t.Fatal("MTU confirmation")
			}
			c.multipath.paths[0].backup = true
			id, err := c.OpenStream(false)
			if err != nil {
				t.Fatal(err)
			}
			payload := make([]byte, 7500)
			for i := range payload {
				payload[i] = byte(i * 71)
			}
			for i := 0; i < 3; i++ {
				if n, err := c.WriteStream(id, payload[i*2500:(i+1)*2500], i == 2); err != nil || n != 2500 {
					t.Fatal("admission", n, err)
				}
			}
			c.multipath.paths[0].backup = false
			var counts [2]int
			large := 0
			var delayed []byte
			for i := 0; i < 300 && c.StreamAcknowledged(id) != nil; i++ {
				now = now.Add(time.Millisecond)
				path, b, err := c.nextMultipathPacket(now)
				if err != nil {
					t.Fatal(err)
				}
				if b != nil {
					if path > 1 {
						t.Fatal("unexpected path", path)
					}
					counts[path]++
					limit := 1200
					if path == 1 {
						limit = 3000
					}
					if len(b) > limit {
						t.Fatal("path MTU exceeded", path, len(b))
					}
					if len(b) > 1200 {
						large++
					}
					if path == 0 && counts[0] == 1 {
						delayed = b
					} else {
						if err := peer.handleMultipathPacket(b, now); err != nil {
							t.Fatal(err)
						}
					}
				}
				if i == 3 && delayed != nil {
					if err := peer.handleMultipathPacket(delayed, now); err != nil {
						t.Fatal("reordered small fragment", err)
					}
					delayed = nil
				}
				now = now.Add(time.Millisecond)
				_, b, err = peer.nextMultipathPacket(now)
				if err != nil {
					t.Fatal(err)
				}
				if b != nil {
					if err := c.handleMultipathPacket(b, now); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := c.StreamAcknowledged(id); err != nil {
				t.Fatal("completion", err)
			}
			if counts[0] == 0 || counts[1] == 0 || large == 0 || delayed != nil || c.queuedBytes != 0 {
				t.Fatal("scheduling/accounting", counts, large, c.queuedBytes)
			}
			for _, path := range []uint64{0, 1} {
				a := c.multipath.paths[path].application
				if c.multipath.abandoned[path] != nil || a.sent.BytesInFlight != 0 {
					t.Fatal("path state", path, a.sent.BytesInFlight)
				}
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
