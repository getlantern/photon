package transport

import (
	"errors"
	"io"
	"testing"

	"github.com/getlantern/photon/internal/stream"
	"github.com/getlantern/photon/internal/wire"
)

// Every endpoint opens both types before either side accepts or releases any
// stream. This exercises actual storage and wire traffic beyond the former
// local, global, incoming, and closed-history caps.
func TestNegotiatedStreamConcurrency(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(vName(version), func(t *testing.T) {
			const count = 602
			p := streamParameters()
			p.InitialMaxStreamsBidi, p.InitialMaxStreamsUni = count, count
			p.InitialMaxStreamDataBidiLocal, p.InitialMaxStreamDataBidiRemote, p.InitialMaxStreamDataUni = 128, 128, 128
			client, server := testPair(t, version, p)
			drivePair(t, client, server, false, false)
			type transfer struct {
				sender, receiver *Conn
				uni              bool
				ids              []uint64
			}
			var transfers []transfer
			for _, peers := range [][2]*Conn{{client, server}, {server, client}} {
				for _, uni := range []bool{false, true} {
					x := transfer{sender: peers[0], receiver: peers[1], uni: uni}
					for i := 0; i < count; i++ {
						id, err := x.sender.OpenStream(uni)
						if err != nil {
							t.Fatalf("open %d: %v", i, err)
						}
						x.ids = append(x.ids, id)
						if n, err := x.sender.WriteStream(id, []byte{byte(i)}, true); err != nil || n != 1 {
							t.Fatalf("write: %d %v", n, err)
						}
					}
					if _, err := x.sender.OpenStream(uni); !errors.Is(err, stream.ErrWouldBlock) {
						t.Fatalf("credit exceeded: %v", err)
					}
					transfers = append(transfers, x)
				}
			}
			drivePair(t, client, server, false, false)
			for _, x := range transfers {
				for i, id := range x.ids {
					got, err := x.receiver.AcceptStream(x.uni)
					if err != nil || got != id {
						t.Fatalf("accept %d: %d %v", id, got, err)
					}
					var data [2]byte
					n, err := x.receiver.ReadStream(id, data[:])
					if n != 1 || data[0] != byte(i) || (err != nil && err != io.EOF) {
						t.Fatalf("data %d: %d %v", id, n, err)
					}
					if _, err := x.receiver.ReadStream(id, data[:]); err != io.EOF {
						t.Fatalf("EOF: %v", err)
					}
					if !x.uni {
						if _, err := x.receiver.WriteStream(id, nil, true); err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			drivePair(t, client, server, false, false)
			for _, x := range transfers {
				if !x.uni {
					for _, id := range x.ids {
						if _, err := x.sender.ReadStream(id, make([]byte, 1)); err != io.EOF {
							t.Fatalf("response EOF: %v", err)
						}
					}
				}
				// Keep 301 holes until the second pass, exceeding the former 257-range cap.
				for _, parity := range []int{1, 0} {
					for i := parity; i < count; i += 2 {
						id := x.ids[i]
						if err := x.sender.ReleaseStream(id); err != nil {
							t.Fatalf("release sender %d: %v", id, err)
						}
						if err := x.receiver.ReleaseStream(id); err != nil {
							t.Fatalf("release receiver %d: %v", id, err)
						}
					}
				}
			}
			drivePair(t, client, server, false, false)
			for _, c := range []*Conn{client, server} {
				if len(c.streams.objects) != 0 {
					t.Fatalf("retained %d streams", len(c.streams.objects))
				}
			}
			for _, x := range transfers {
				for i := 0; i < count; i++ {
					if _, err := x.sender.OpenStream(x.uni); err != nil {
						t.Fatalf("replenished %d: %v", i, err)
					}
				}
				if _, err := x.sender.OpenStream(x.uni); !errors.Is(err, stream.ErrWouldBlock) {
					t.Fatalf("excess replenished credit: %v", err)
				}
			}
		})
	}
}
