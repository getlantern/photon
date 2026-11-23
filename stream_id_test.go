package quic_test

import (
	quic "github.com/getlantern/photon"
	"testing"
)

func TestStreamIDOrdinal(t *testing.T) {
	for _, test := range []struct {
		id      quic.StreamID
		ordinal int64
	}{
		{0, 1}, {1, 1}, {2, 1}, {3, 1}, {4, 2}, {7, 2}, {8, 3}, {9, 3},
		{(1 << 62) - 4, 1 << 60}, {(1 << 62) - 1, 1 << 60},
	} {
		if got := int64(test.id.StreamNum()); got != test.ordinal {
			t.Fatalf("ID %d: ordinal %d, want %d", test.id, got, test.ordinal)
		}
	}
	// The return value's conversion method is usable by an external package,
	// even though its named argument types live in the implementation's internal package.
	number := quic.StreamID(9).StreamNum()
	if number.StreamID(1, 2) != 8 || number.StreamID(1, 1) != 9 || number.StreamID(0, 2) != 10 || number.StreamID(0, 1) != 11 {
		t.Fatal("ordinal did not preserve all four stream classes")
	}
	if quic.StreamID(-4).StreamNum().StreamID(0, 2) != -1 {
		t.Fatal("zero count did not produce invalid stream ID")
	}
}
