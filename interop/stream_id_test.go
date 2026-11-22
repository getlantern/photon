package interop

import (
	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
	"testing"
)

func TestStreamIDOrdinalCompatibility(t *testing.T) {
	ids := []int64{-1, -4, -5, -(1 << 63), 1<<63 - 1, 1<<62 - 1, 1<<62 - 4}
	for id := int64(0); id < 1024; id++ {
		ids = append(ids, id)
	}
	for _, id := range ids {
		own := quic.StreamID(id).StreamNum()
		reference := upstream.StreamID(id).StreamNum()
		if int64(own) != int64(reference) {
			t.Fatalf("ID %d: own ordinal %d, upstream %d", id, own, reference)
		}
		ownIDs := [6]int64{int64(own.StreamID(0, 1)), int64(own.StreamID(0, 2)), int64(own.StreamID(1, 1)), int64(own.StreamID(1, 2)), int64(own.StreamID(3, 1)), int64(own.StreamID(0, -1))}
		referenceIDs := [6]int64{int64(reference.StreamID(0, 1)), int64(reference.StreamID(0, 2)), int64(reference.StreamID(1, 1)), int64(reference.StreamID(1, 2)), int64(reference.StreamID(3, 1)), int64(reference.StreamID(0, -1))}
		if ownIDs != referenceIDs {
			t.Fatalf("ID %d: reconstructed own IDs %v, upstream %v", id, ownIDs, referenceIDs)
		}
	}
}
