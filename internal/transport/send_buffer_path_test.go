package transport

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/wire"
)

// Use real packet sealing, receive dispatch and stream state with a synthetic
// negotiated multipath owner. Inject loss by queuing the original item on path
// one, then acknowledge its retransmission over path zero and its original over
// path one after the payload buffer has been reused by a different stream.
func TestSendBufferCrossPathLateACK(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(version), func(t *testing.T) {
			client, server := sendingPathPair(t, version)
			now := time.Unix(101, 0)
			for _, c := range []*Conn{client, server} {
				c.multipath.paths[1].application.budget.Validate()
			}
			id, err := client.OpenStream(false)
			if err != nil {
				t.Fatal(err)
			}
			want := bytes.Repeat([]byte{0x41}, 1000)
			if n, err := client.WriteStream(id, want, true); err != nil || n != len(want) {
				t.Fatal(n, err)
			}
			source := client.multipath.paths[0].application.space
			item := source.queue[len(source.queue)-1]
			retained := item.frame.(wire.StreamFrame).Data
			// Bypass pending ACK/control output until this STREAM item is transmitted.
			sendItemOnPath := func(path uint64, target *sendItem) ([]byte, uint64) {
				t.Helper()
				s := client.multipath.paths[path].application.space
				for attempt := 0; attempt < 16; attempt++ {
					pn := s.nextPN
					raw, err := client.nextPacketOnPath(path, now)
					now = now.Add(time.Millisecond)
					if err != nil {
						t.Fatal(err)
					}
					if raw != nil && s.sent[pn] == target {
						return raw, pn
					}
				}
				t.Fatal("stream was not transmitted")
				return nil, 0
			}
			original, originalPN := sendItemOnPath(0, item)
			client.multipath.paths[1].application.validation.state = PathValidated
			destination := client.multipath.paths[1].application.space
			queueLostItems(source, destination, []recovery.Packet{{Number: originalPN}})
			retransmission, retransmissionPN := sendItemOnPath(1, item)
			if err := server.handleMultipathPacket(retransmission, now); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(want))
			if n, err := server.ReadStream(id, got); err != io.EOF || n != len(want) || !bytes.Equal(got, want) {
				t.Fatal("retransmission payload", n, err)
			}
			acknowledge := func(named, received, pn, ackPN uint64) {
				t.Helper()
				payload, err := (wire.PathAckFrame{PathID: named, AckFrame: wire.AckFrame{Ranges: []wire.AckRange{{Smallest: pn, Largest: pn}}}}).Append(nil)
				if err != nil {
					t.Fatal(err)
				}
				cid := client.config.LocalCID
				if received != 0 {
					for _, entry := range client.multipath.paths[received].local.active {
						cid = entry.cid
						break
					}
				}
				raw := sealPathPayload(t, server.writeKeys(applicationSpace), cid, uint32(received), ackPN, payload)
				if err := client.handleMultipathPacket(raw, now); err != nil {
					t.Fatal(err)
				}
				now = now.Add(time.Millisecond)
			}
			acknowledge(1, 0, retransmissionPN, 100)
			if !item.acked || item.frame != nil || len(client.sendBuffers) != 1 {
				t.Fatal("cross-path ACK did not recycle completed payload")
			}
			second, err := client.OpenStream(false)
			if err != nil {
				t.Fatal(err)
			}
			fresh := bytes.Repeat([]byte{0x42}, len(want))
			if n, err := client.WriteStream(second, fresh, true); err != nil || n != len(fresh) {
				t.Fatal(n, err)
			}
			freshPath := client.multipath.lastEnqueued
			freshSpace := client.multipath.paths[freshPath].application.space
			next := freshSpace.queue[len(freshSpace.queue)-1]
			nextFrame := next.frame.(wire.StreamFrame)
			if &nextFrame.Data[0] != &retained[0] {
				t.Fatal("test did not reuse original buffer")
			}
			before := client.queuedBytes
			acknowledge(0, 1, originalPN, 100)
			acknowledge(1, 0, retransmissionPN, 101)
			if client.queuedBytes != before || next.acked || len(client.sendBuffers) != 0 || !bytes.Equal(nextFrame.Data, fresh) {
				t.Fatal("late ACK retired or corrupted fresh write")
			}
			// Previously sealed output owns its bytes even after retained input reuse.
			if err := server.handleMultipathPacket(original, now); err != nil {
				t.Fatal("delayed original packet", err)
			}
			raw, _ := sendItemOnPath(freshPath, next)
			if err := server.handleMultipathPacket(raw, now); err != nil {
				t.Fatal(err)
			}
			if n, err := server.ReadStream(second, got); err != io.EOF || n != len(fresh) || !bytes.Equal(got, fresh) {
				t.Fatal("fresh payload after late ACK", n, err)
			}
		})
	}
}
