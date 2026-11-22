package wire

import (
	"bytes"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"
)

func multipathFixtures() []struct {
	frame Frame
	hex   string
} {
	ack := AckFrame{Delay: 4, Ranges: []AckRange{{8, 10}, {2, 4}}}
	ecn := ack
	ecn.ECN = &[3]uint64{1, 2, 3}
	return []struct {
		frame Frame
		hex   string
	}{
		{PathAckFrame{3, ack}, "3e030a0401020202"},
		{PathAckFrame{3, ecn}, "3f030a0401020202010203"},
		{PathAbandonFrame{3, 0}, "7e750300"},
		{PathStatusFrame{PathID: 3, Sequence: 7, Backup: true}, "7e760307"},
		{PathStatusFrame{PathID: 3, Sequence: 7}, "7e770307"},
		{PathNewConnectionIDFrame{3, NewConnectionIDFrame{Sequence: 7, RetirePriorTo: 2, ConnectionID: []byte{0xab}}}, "7e7803070201ab00000000000000000000000000000000"},
		{PathRetireConnectionIDFrame{3, 7}, "7e790307"},
		{MaxPathIDFrame{3}, "7e7a03"},
		{PathsBlockedFrame{3}, "7e7b03"},
		{PathCIDsBlockedFrame{3, 7}, "7e7c0307"},
	}
}

func TestMultipathFrameFormats(t *testing.T) {
	for _, tc := range multipathFixtures() {
		t.Run(tc.hex, func(t *testing.T) {
			want, err := hex.DecodeString(tc.hex)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := tc.frame.Append(nil)
			if err != nil || !bytes.Equal(encoded, want) {
				t.Fatalf("encoding %x: %v, want %x", encoded, err, want)
			}
			// A following PING must stay outside the consumed frame.
			parsed, used, err := ConsumeFrame(append(bytes.Clone(want), 1))
			if err != nil || used != len(want) || !reflect.DeepEqual(parsed, tc.frame) {
				t.Fatalf("parsed %#v used %d: %v", parsed, used, err)
			}
			for n := 0; n < len(want); n++ {
				if _, used, err := ConsumeFrame(want[:n]); err == nil || used != 0 {
					t.Fatalf("accepted prefix %d", n)
				}
			}
			for _, typ := range []PacketType{Initial, Handshake, ZeroRTT, OneRTT, Retry} {
				for _, enabled := range []bool{false, true} {
					for _, server := range []bool{false, true} {
						err := ValidateFrame(parsed, FrameContext{PacketType: typ, SenderIsServer: server, MultipathNegotiated: enabled}, used)
						if (err == nil) != (typ == OneRTT && enabled) {
							t.Fatalf("context type %v enabled %v server %v: %v", typ, enabled, server, err)
						}
					}
				}
			}
			wantEliciting := tc.frame.Type() != FramePathAck && tc.frame.Type() != FramePathAckECN
			if AckEliciting(parsed) != wantEliciting || InFlightFrame(parsed) != wantEliciting {
				t.Fatal("ACK/in-flight classification")
			}
			// Frame types must use minimal varints even when other fields need not.
			typ := tc.frame.Type()
			nonminimal := []byte{0x80, 0, byte(typ >> 8), byte(typ)}
			nonminimal = append(nonminimal, want[VarintLen(typ):]...)
			if _, _, err := ConsumeFrame(nonminimal); !errors.Is(err, ErrFrame) {
				t.Fatal("nonminimal type accepted")
			}
		})
	}
}

func TestMultipathInvalidEncodingAtomicity(t *testing.T) {
	frames := []Frame{
		PathAckFrame{PathID: MaxVarint + 1, AckFrame: AckFrame{Ranges: []AckRange{{0, 0}}}},
		PathAckFrame{PathID: 1},
		PathAckFrame{AckFrame: AckFrame{Ranges: []AckRange{{2, 1}}}},
		PathAckFrame{AckFrame: AckFrame{Ranges: []AckRange{{0, 0}}, ECN: &[3]uint64{MaxVarint + 1, 0, 0}}},
		PathAbandonFrame{1, MaxVarint + 1},
		PathStatusFrame{PathID: MaxVarint + 1},
		PathNewConnectionIDFrame{1, NewConnectionIDFrame{}},
		PathNewConnectionIDFrame{1, NewConnectionIDFrame{Sequence: 1, RetirePriorTo: 2, ConnectionID: []byte{1}}},
		PathNewConnectionIDFrame{MaxVarint + 1, NewConnectionIDFrame{ConnectionID: []byte{1}}},
		PathRetireConnectionIDFrame{1, MaxVarint + 1},
		MaxPathIDFrame{MaxVarint + 1}, PathsBlockedFrame{MaxVarint + 1}, PathCIDsBlockedFrame{1, MaxVarint + 1},
	}
	for _, f := range frames {
		backing := bytes.Repeat([]byte{0xcc}, 128)
		before := bytes.Clone(backing)
		out, err := f.Append(backing[:2])
		if !errors.Is(err, ErrFrame) || len(out) != 2 || !bytes.Equal(backing, before) {
			t.Fatalf("%T changed output on failure: %v", f, err)
		}
	}
	// Values outside negotiated path limits are syntactically valid. Preserve
	// them for the connection's PROTOCOL_VIOLATION checks, without truncation.
	f := MaxPathIDFrame{1 << 32}
	b, _ := f.Append(nil)
	parsed, _, err := ConsumeFrame(b)
	if err != nil || parsed != f {
		t.Fatalf("lost out-of-limit path ID: %#v %v", parsed, err)
	}
	for _, s := range []string{
		"3e0100010001", // first ACK range exceeds largest
		"3e0100013f00", // impossible ACK range count
		"7e7801000000", // zero-length CID
		"7e7801000015", // CID too long
		"7e7801000101", // retire-prior exceeds sequence
	} {
		b, _ := hex.DecodeString(s)
		if _, used, err := ConsumeFrame(b); err == nil || used != 0 {
			t.Fatalf("accepted malformed %s", s)
		}
	}
}

func FuzzMultipathFrames(f *testing.F) {
	for _, tc := range multipathFixtures() {
		b, _ := hex.DecodeString(tc.hex)
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		before := bytes.Clone(b)
		frame, used, err := ConsumeFrame(b)
		if !bytes.Equal(b, before) {
			t.Fatal("mutated input")
		}
		if err != nil {
			if used != 0 {
				t.Fatal("consumed error")
			}
			return
		}
		if used <= 0 || used > len(b) {
			t.Fatal("invalid consumed length")
		}
		encoded, err := frame.Append(nil)
		if err != nil {
			t.Fatal(err)
		}
		again, n, err := ConsumeFrame(encoded)
		if err != nil || n != len(encoded) || !reflect.DeepEqual(frame, again) {
			t.Fatalf("roundtrip %T: %v", frame, err)
		}
	})
}
