package transport

import (
	"bytes"
	"errors"
	"testing"

	"github.com/getlantern/photon/internal/recovery"
	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

// Exercise logical ownership with two packet histories sharing retransmitted
// items. Packet protection and ACK-of-unsent validation belong to other gates.
func FuzzSendBufferOwnership(f *testing.F) {
	f.Add([]byte{0, 0, 80, 1, 0, 0, 0, 0, 80, 2, 0, 0, 1, 0, 0, 3, 1, 0})
	f.Add([]byte{0, 0, 10, 2, 0, 0, 4, 1, 0, 1, 1, 0, 0, 0, 10, 3, 1, 0, 1, 0, 0})
	f.Fuzz(func(t *testing.T, actions []byte) {
		if len(actions) > 768 {
			actions = actions[:768]
		}
		c := &Conn{}
		spaces := []*numberSpace{{sent: make(map[uint64]*sendItem)}, {sent: make(map[uint64]*sendItem)}}
		type record struct {
			item        *sendItem
			want        []byte
			state       byte
			completions int
		}
		type transmission struct {
			path  int
			pn    uint64
			index int
		}
		var records []record
		var transmissions []transmission
		send := func(path, index int) {
			s := spaces[path]
			pn := s.nextPN
			s.nextPN++
			s.sent[pn] = records[index].item
			records[index].item.queued = false
			transmissions = append(transmissions, transmission{path, pn, index})
		}
		refused := errors.New("completion refused")
		for step := 0; step+2 < len(actions); step += 3 {
			op, selector, value := actions[step]%6, int(actions[step+1]), int(actions[step+2])
			if op == 0 {
				size := 1 + (value*13)%2048
				input := bytes.Repeat([]byte{byte(len(records) + 1)}, size)
				want := append([]byte(nil), input...)
				flow, _ := stream.NewSendFlow(uint64(size))
				sender, _ := stream.NewSend(uint64(len(records))*4, uint64(size), flow)
				frame, n, err := sender.WriteBuffer(input, true, size, c.takeSendBuffer(size))
				if err != nil || n != size {
					t.Fatal(n, err)
				}
				clear(input)
				item := &sendItem{frame: frame, bytes: size + 32}
				c.queuedBytes += item.bytes
				records = append(records, record{item: item, want: want})
				send(value%2, len(records)-1)
			} else if len(transmissions) > 0 {
				tx := transmissions[selector%len(transmissions)]
				r := &records[tx.index]
				switch op {
				case 1, 4: // Successful or refused completion, including late/duplicate ACKs.
					prior := r.state
					err := acknowledgeFrame(spaces[tx.path], wire.AckFrame{Ranges: []wire.AckRange{{Smallest: tx.pn, Largest: tx.pn}}}, func(item *sendItem) error {
						if op == 4 {
							return refused
						}
						if prior != 0 || item != r.item {
							t.Fatal("unexpected completion")
						}
						r.completions++
						c.queuedBytes -= item.bytes
						c.recycleStreamItem(item)
						return nil
					})
					if op == 4 && prior == 0 {
						if !errors.Is(err, refused) {
							t.Fatal("missing refused completion", err)
						}
					} else if err != nil {
						t.Fatal(err)
					}
					if op == 1 && prior == 0 {
						r.state = 1
					}
				case 2: // Loss can move a retransmission to the other packet history.
					destination := spaces[1-tx.path]
					queueLostItems(spaces[tx.path], destination, []recovery.Packet{{Number: tx.pn}})
					if r.state != 0 && len(destination.queue) != 0 {
						t.Fatal("retired data requeued")
					}
					if len(destination.queue) > 0 {
						if len(destination.queue) != 1 || destination.queue[0] != r.item {
							t.Fatal("wrong retransmission")
						}
						destination.queue = nil
						send(1-tx.path, tx.index)
					}
				case 3: // Cancellation does not recycle the retained payload.
					for _, s := range spaces {
						c.retirePathFrames(s, func(frame wire.Frame) bool {
							v, ok := frame.(wire.StreamFrame)
							return ok && v.StreamID == uint64(tx.index)*4
						})
					}
					if r.state == 0 {
						r.state = 2
					}
				case 5: // Prune one path while retaining stale mappings on the other.
					pruneAcknowledged(spaces[tx.path])
				}
			}
			wantCharge := 0
			occupied := make(map[*byte]bool)
			for i := range records {
				r := &records[i]
				if r.state == 0 {
					wantCharge += r.item.bytes
					frame, ok := r.item.frame.(wire.StreamFrame)
					if !ok || !bytes.Equal(frame.Data, r.want) || r.item.acked || r.completions != 0 {
						t.Fatal("live payload or ownership changed", i)
					}
					occupied[&frame.Data[0]] = true
				} else if !r.item.acked || (r.state == 1 && r.completions != 1) || (r.state == 2 && r.completions != 0) {
					t.Fatal("retirement mismatch", i)
				}
				if r.state == 1 && r.item.frame != nil {
					t.Fatal("completed item retained frame", i)
				}
				if r.state == 2 {
					frame, ok := r.item.frame.(wire.StreamFrame)
					if !ok || !bytes.Equal(frame.Data, r.want) {
						t.Fatal("canceled payload was recycled", i)
					}
					occupied[&frame.Data[0]] = true
				}
			}
			if c.queuedBytes != wantCharge {
				t.Fatal("send accounting mismatch", c.queuedBytes, wantCharge)
			}
			if len(c.sendBuffers) > maxCachedSendBuffers {
				t.Fatal("cache overflow")
			}
			cacheBytes := 0
			for _, buffer := range c.sendBuffers {
				cacheBytes += cap(buffer)
				if cap(buffer) == 0 || cap(buffer) > maxCachedSendBufferSize {
					t.Fatal("invalid cache capacity")
				}
				ptr := &buffer[:cap(buffer)][0]
				if occupied[ptr] {
					t.Fatal("cache aliases live payload or another cache entry")
				}
				occupied[ptr] = true
			}
			if cacheBytes != c.cachedSendBufferBytes || cacheBytes > maxCachedSendBufferBytes {
				t.Fatal("cache byte accounting", cacheBytes, c.cachedSendBufferBytes)
			}
		}
	})
}
