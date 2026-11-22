package transport

import (
	"bytes"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestACKViewEncodingAndSnapshotOwnership(t *testing.T) {
	for _, ecn := range []ECN{ECNUnknown, ECNECT0, ECNCE} {
		received, _ := NewReceived(4)
		now := time.Unix(100, 0)
		for _, pn := range []uint64{1, 3, 5} {
			if _, err := received.RecordECN(pn, now, ecn); err != nil {
				t.Fatal(err)
			}
		}
		snapshot, err := received.Ack(now.Add(time.Millisecond), 3)
		if err != nil {
			t.Fatal(err)
		}
		want, err := snapshot.Append(nil)
		if err != nil {
			t.Fatal(err)
		}
		view, err := received.ackView(now.Add(time.Millisecond), 3)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := view.Append(nil)
		if err != nil || !bytes.Equal(encoded, want) {
			t.Fatal("view encoding", err)
		}
		// Bridge ranges and update ECN after encoding. Owned snapshots and bytes
		// already serialized for protection must remain independent of the tracker.
		received.RecordECN(4, now.Add(time.Millisecond), ECNCE)
		received.RecordECN(6, now.Add(time.Millisecond), ECNECT1)
		after, err := snapshot.Append(nil)
		if err != nil || !bytes.Equal(after, want) || !bytes.Equal(encoded, want) {
			t.Fatal("snapshot mutated", err)
		}
		frame, _, err := wire.ConsumeFrame(encoded)
		if err != nil {
			t.Fatal(err)
		}
		ack := frame.(wire.AckFrame)
		if len(ack.Ranges) != 3 || ack.Ranges[0].Largest != 5 {
			t.Fatal(ack)
		}
	}
}

var ackEncodingSink []byte

func BenchmarkACKEncoding(b *testing.B) {
	for _, borrowed := range []bool{false, true} {
		name := "snapshot"
		if borrowed {
			name = "borrowed"
		}
		b.Run(name, func(b *testing.B) {
			r, _ := NewReceived(32)
			now := time.Unix(100, 0)
			for _, pn := range []uint64{1, 3, 5, 7} {
				r.RecordECN(pn, now, ECNECT0)
			}
			storage := make([]byte, 0, 128)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var f wire.AckFrame
				var err error
				if borrowed {
					f, err = r.ackView(now, 3)
				} else {
					f, err = r.Ack(now, 3)
				}
				if err != nil {
					b.Fatal(err)
				}
				ackEncodingSink, err = f.Append(storage[:0])
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
