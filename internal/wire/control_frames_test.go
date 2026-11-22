package wire

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
)

func frameVectors() []struct {
	frame Frame
	hex   string
} {
	return []struct {
		frame Frame
		hex   string
	}{
		{PaddingFrame{}, "00"}, {PingFrame{}, "01"}, {HandshakeDoneFrame{}, "1e"},
		{AckFrame{Ranges: []AckRange{{0, 0}}}, "0200000000"},
		{ResetStreamFrame{StreamID: 4, ErrorCode: 42, FinalSize: 1000}, "04042a43e8"},
		{ResetStreamAtFrame{StreamID: 4, ErrorCode: 42, FinalSize: 1000, ReliableSize: 64}, "24042a43e84040"},
		{StopSendingFrame{StreamID: 5, ErrorCode: 42}, "05052a"},
		{CryptoFrame{Offset: 1, Data: []byte("abc")}, "060103616263"},
		{NewTokenFrame{Token: []byte("tok")}, "0703746f6b"},
		{StreamFrame{StreamID: 8, Offset: 2, Data: []byte("hi"), Fin: true}, "0f0802026869"},
		{MaxDataFrame{Maximum: 1000}, "1043e8"},
		{MaxStreamDataFrame{StreamID: 8, Maximum: 1000}, "110843e8"},
		{MaxStreamsFrame{Maximum: 64}, "124040"},
		{MaxStreamsFrame{Unidirectional: true, Maximum: 64}, "134040"},
		{DataBlockedFrame{Limit: 1000}, "1443e8"},
		{StreamDataBlockedFrame{StreamID: 8, Limit: 1000}, "150843e8"},
		{StreamsBlockedFrame{Limit: 64}, "164040"},
		{StreamsBlockedFrame{Unidirectional: true, Limit: 64}, "174040"},
		{NewConnectionIDFrame{Sequence: 3, RetirePriorTo: 2, ConnectionID: []byte{1, 2}, StatelessResetToken: [16]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}}, "180302020102000102030405060708090a0b0c0d0e0f"},
		{RetireConnectionIDFrame{Sequence: 3}, "1903"},
		{PathChallengeFrame{Data: [8]byte{0, 1, 2, 3, 4, 5, 6, 7}}, "1a0001020304050607"},
		{PathResponseFrame{Data: [8]byte{0, 1, 2, 3, 4, 5, 6, 7}}, "1b0001020304050607"},
		{ConnectionCloseFrame{ErrorCode: 10, TriggerFrame: 6, Reason: []byte("bad")}, "1c0a0603626164"},
		{ConnectionCloseFrame{Application: true, ErrorCode: 42, Reason: []byte("bye")}, "1d2a03627965"},
		{DatagramFrame{Data: []byte("hi")}, "31026869"},
		{DatagramFrame{Data: []byte("hi"), OmitLength: true}, "306869"},
	}
}

func TestControlFrameVectors(t *testing.T) {
	for _, tc := range frameVectors() {
		t.Run(tc.hex, func(t *testing.T) {
			want := decodeHex(t, tc.hex)
			got, err := tc.frame.Append(nil)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("encode %T: %x %v", tc.frame, got, err)
			}
			before := bytes.Clone(want)
			frame, n, err := ConsumeFrame(want)
			if err != nil || n != len(want) || !reflect.DeepEqual(frame, tc.frame) {
				t.Fatalf("decode: %#v %d %v", frame, n, err)
			}
			if !bytes.Equal(before, want) {
				t.Fatal("mutated input")
			}
			if frame.Type() != tc.frame.Type() {
				t.Fatal("wrong frame type")
			}
			if frame.Type() == 0x30 {
				return
			} // Remainder-of-packet DATAGRAM can be empty.
			for i := 0; i < len(want); i++ {
				frame, n, err := ConsumeFrame(want[:i])
				if !errors.Is(err, ErrFrame) || frame != nil || n != 0 {
					t.Fatalf("accepted truncated frame at %d: %#v %d %v", i, frame, n, err)
				}
			}
			// All explicit-length vectors leave following frames untouched.
			frame, n, err = ConsumeFrame(append(want, 1))
			if err != nil || n != len(want) {
				t.Fatalf("consumed next frame: %d %v", n, err)
			}
		})
	}
}

func TestControlFrameInvalidEncodings(t *testing.T) {
	for _, hex := range []string{
		"0700",               // empty NEW_TOKEN
		"18000000",           // zero-length CID
		"18000015",           // 21-byte CID
		"18000101",           // retire_prior_to greater than sequence
		"12d000000000000001", // stream count > 2^60
		"17d000000000000001", // blocked stream count > 2^60
		"4001",               // nonminimal frame type
		"80000001", "c000000000000001",
		"1f", "403f", "4040", // unknown frame types
		"310201", // truncated datagram
	} {
		frame, n, err := ConsumeFrame(decodeHex(t, hex))
		var fe *FrameError
		if frame != nil || n != 0 || !errors.As(err, &fe) || !errors.Is(err, ErrFrame) {
			t.Fatalf("accepted %s: %v", hex, err)
		}
	}
	_, _, err := ConsumeFrame([]byte{0x1a, 1})
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("lost truncation cause", err)
	}
	for _, frame := range []Frame{
		ResetStreamFrame{FinalSize: MaxVarint + 1}, StopSendingFrame{StreamID: MaxVarint + 1},
		NewTokenFrame{}, MaxDataFrame{Maximum: MaxVarint + 1}, MaxStreamDataFrame{StreamID: MaxVarint + 1},
		MaxStreamsFrame{Maximum: 1<<60 + 1}, DataBlockedFrame{Limit: MaxVarint + 1},
		StreamDataBlockedFrame{Limit: MaxVarint + 1}, StreamsBlockedFrame{Limit: 1<<60 + 1},
		NewConnectionIDFrame{}, NewConnectionIDFrame{Sequence: 1, RetirePriorTo: 2, ConnectionID: []byte{1}},
		NewConnectionIDFrame{ConnectionID: make([]byte, 21)}, RetireConnectionIDFrame{Sequence: MaxVarint + 1},
		ConnectionCloseFrame{ErrorCode: MaxVarint + 1}, ConnectionCloseFrame{Application: true, TriggerFrame: 1},
	} {
		backing := bytes.Repeat([]byte{0xaa}, 64)
		before := bytes.Clone(backing)
		out, err := frame.Append(backing[:1])
		if err == nil || !bytes.Equal(out, before[:1]) || !bytes.Equal(backing, before) {
			t.Fatalf("invalid %T mutated destination or was accepted", frame)
		}
	}
}

func TestFrameContexts(t *testing.T) {
	// Independent RFC 9000 sections 12.4/12.5 matrix: Initial, Handshake,
	// client 0-RTT, then 1-RTT. Section 12.5 prohibits RETIRE_CONNECTION_ID in
	// 0-RTT even though the summary table marks that row __01.
	for _, tc := range []struct {
		typ     byte
		allowed [4]bool
	}{
		{0, [4]bool{true, true, true, true}}, {1, [4]bool{true, true, true, true}},
		{2, [4]bool{true, true, false, true}}, {3, [4]bool{true, true, false, true}},
		{4, [4]bool{false, false, true, true}}, {5, [4]bool{false, false, true, true}},
		{6, [4]bool{true, true, false, true}}, {7, [4]bool{false, false, false, true}},
		{8, [4]bool{false, false, true, true}}, {15, [4]bool{false, false, true, true}},
		{0x10, [4]bool{false, false, true, true}}, {0x11, [4]bool{false, false, true, true}},
		{0x12, [4]bool{false, false, true, true}}, {0x13, [4]bool{false, false, true, true}},
		{0x14, [4]bool{false, false, true, true}}, {0x15, [4]bool{false, false, true, true}},
		{0x16, [4]bool{false, false, true, true}}, {0x17, [4]bool{false, false, true, true}},
		{0x18, [4]bool{false, false, true, true}}, {0x19, [4]bool{false, false, false, true}},
		{0x1a, [4]bool{false, false, true, true}}, {0x1b, [4]bool{false, false, false, true}},
		{0x1c, [4]bool{true, true, true, true}}, {0x1d, [4]bool{false, false, true, true}},
		{0x1e, [4]bool{false, false, false, true}}, {0x30, [4]bool{false, false, true, true}},
		{0x31, [4]bool{false, false, true, true}},
	} {
		// Type-only test frame isolates classification from encoding tests above.
		f := typeOnlyFrame(tc.typ)
		for i, packet := range []PacketType{Initial, Handshake, ZeroRTT, OneRTT} {
			ctx := FrameContext{PacketType: packet, SenderIsServer: tc.typ == 7 || tc.typ == 0x1e, MaxDatagramFrameSize: 100}
			if got := ValidateFrame(f, ctx, 10) == nil; got != tc.allowed[i] {
				t.Fatalf("type %x packet %v: allowed %v", tc.typ, packet, got)
			}
		}
	}
	for _, f := range []Frame{NewTokenFrame{Token: []byte{1}}, HandshakeDoneFrame{}} {
		if err := ValidateFrame(f, FrameContext{PacketType: OneRTT}, 2); !errors.Is(err, ErrFrameUnexpected) {
			t.Fatal("accepted client-only violation")
		}
	}
	for _, ctx := range []FrameContext{{PacketType: Retry}, {PacketType: 99}, {PacketType: ZeroRTT, SenderIsServer: true}} {
		if err := ValidateFrame(PingFrame{}, ctx, 1); err == nil {
			t.Fatal("invalid packet context")
		}
	}
	d := DatagramFrame{Data: []byte{1}}
	for _, limit := range []uint64{0, 2} {
		if err := ValidateFrame(d, FrameContext{PacketType: OneRTT, MaxDatagramFrameSize: limit}, 3); err == nil {
			t.Fatal("datagram limit")
		}
	}
	// Wire length can be longer than a minimal re-encoding due to a nonminimal
	// length integer. Enforce the actual consumed frame size.
	frame, n, err := ConsumeFrame([]byte{0x31, 0x40, 1, 42})
	if err != nil || n != 4 {
		t.Fatal(err)
	}
	if err := ValidateFrame(frame, FrameContext{PacketType: OneRTT, MaxDatagramFrameSize: 3}, n); err == nil {
		t.Fatal("underestimated DATAGRAM size")
	}
	if err := ValidateFrame(frame, FrameContext{PacketType: OneRTT, MaxDatagramFrameSize: 4}, n); err != nil {
		t.Fatal(err)
	}
}

type typeOnlyFrame byte

func (f typeOnlyFrame) Type() uint64                    { return uint64(f) }
func (f typeOnlyFrame) Append(b []byte) ([]byte, error) { return append(b, byte(f)), nil }

func TestFrameProperties(t *testing.T) {
	for _, tc := range []struct {
		f                  Frame
		ack, probe, flight bool
	}{
		{PaddingFrame{}, false, true, true}, {PingFrame{}, true, false, true},
		{AckFrame{}, false, false, false}, {ConnectionCloseFrame{}, false, false, false},
		{ConnectionCloseFrame{Application: true}, false, false, false},
		{PathChallengeFrame{}, true, true, true}, {PathResponseFrame{}, true, true, true},
		{NewConnectionIDFrame{}, true, true, true}, {StreamFrame{}, true, false, true},
		{DatagramFrame{}, true, false, true}, {CryptoFrame{}, true, false, true},
	} {
		if AckEliciting(tc.f) != tc.ack || ProbingFrame(tc.f) != tc.probe || InFlightFrame(tc.f) != tc.flight {
			t.Fatalf("wrong classification for %T", tc.f)
		}
	}
}

func FuzzFrameParser(f *testing.F) {
	for _, tc := range frameVectors() {
		f.Add(decodeHex(f, tc.hex))
	}
	f.Add([]byte{0x40})
	f.Fuzz(func(t *testing.T, b []byte) {
		frame, n, err := ConsumeFrame(b)
		if err != nil {
			if frame != nil || n != 0 {
				t.Fatal("error consumed input")
			}
			return
		}
		if n < 1 || n > len(b) {
			t.Fatal("bounds")
		}
		encoded, err := frame.Append(nil)
		if err != nil {
			t.Fatal(err)
		}
		again, used, err := ConsumeFrame(encoded)
		if err != nil || used != len(encoded) {
			t.Fatal("roundtrip", err)
		}
		canonical, err := again.Append(nil)
		if err != nil || !bytes.Equal(encoded, canonical) {
			t.Fatal("unstable encoding")
		}
	})
}
