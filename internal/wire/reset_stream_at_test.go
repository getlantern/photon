package wire

import (
	"bytes"
	"errors"
	"testing"
)

func TestResetStreamAtBounds(t *testing.T) {
	for _, value := range []uint64{0, 63, 64, 16383, 16384, 1<<30 - 1, 1 << 30, MaxVarint} {
		frame := ResetStreamAtFrame{value, value, value, value}
		encoded, err := frame.Append(nil)
		if err != nil {
			t.Fatal(err)
		}
		decoded, n, err := ConsumeFrame(encoded)
		if err != nil || n != len(encoded) || decoded != frame {
			t.Fatal(value, decoded, n, err)
		}
	}
	for _, frame := range []ResetStreamAtFrame{
		{FinalSize: 0, ReliableSize: 1}, {FinalSize: MaxVarint - 1, ReliableSize: MaxVarint},
		{StreamID: MaxVarint + 1}, {ErrorCode: MaxVarint + 1}, {FinalSize: MaxVarint + 1},
		{FinalSize: MaxVarint + 1, ReliableSize: MaxVarint + 1},
	} {
		storage := bytes.Repeat([]byte{0xa5}, 80)
		before := bytes.Clone(storage)
		got, err := frame.Append(storage[:2])
		if !errors.Is(err, ErrFrame) || !bytes.Equal(got, before[:2]) || !bytes.Equal(storage, before) {
			t.Fatal("invalid frame mutated output", frame, err)
		}
	}
	for _, input := range [][]byte{{0x24, 0, 0, 0, 1}, {0x40, 0x24, 0, 0, 0, 0}} {
		frame, n, err := ConsumeFrame(input)
		var failure *FrameError
		if frame != nil || n != 0 || !errors.As(err, &failure) || failure.FrameType != FrameResetStreamAt {
			t.Fatal("invalid frame accepted", frame, n, err)
		}
	}
}

func TestResetStreamAtContext(t *testing.T) {
	frame := ResetStreamAtFrame{}
	for _, packet := range []PacketType{Initial, Handshake, ZeroRTT, OneRTT, Retry} {
		for _, server := range []bool{false, true} {
			for _, enabled := range []bool{false, true} {
				allowed := enabled && (packet == OneRTT || (packet == ZeroRTT && !server))
				err := ValidateFrame(frame, FrameContext{PacketType: packet, SenderIsServer: server, ResetStreamAtEnabled: enabled}, 5)
				if (err == nil) != allowed {
					t.Fatal(packet, server, enabled, err)
				}
			}
		}
	}
	if !AckEliciting(frame) || !InFlightFrame(frame) || ProbingFrame(frame) {
		t.Fatal("reset classification")
	}
}

func TestResetStreamAtParameter(t *testing.T) {
	for _, server := range []bool{false, true} {
		p := DefaultTransportParameters()
		p.InitialSourceConnectionID = []byte{1}
		if server {
			p.OriginalDestinationConnectionID = []byte{2}
		}
		for _, enabled := range []bool{false, true} {
			p.ResetStreamAt = enabled
			encoded, err := p.Marshal(server)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := ParseTransportParameters(encoded, server)
			if err != nil || decoded.ResetStreamAt != enabled {
				t.Fatal(server, enabled, err)
			}
		}
	}
	for _, suffix := range []string{"1d0100", "1d020000", "1d001d00"} {
		input := append([]byte{15, 1, 1}, decodeHex(t, suffix)...)
		_, err := ParseTransportParameters(input, false)
		var failure *ParameterError
		if !errors.As(err, &failure) || failure.ID != 0x1d {
			t.Fatal(suffix, err)
		}
	}
	// TP IDs and lengths may use nonminimal encodings; frame types may not.
	p, err := ParseTransportParameters([]byte{15, 1, 1, 0x40, 0x1d, 0x40, 0}, false)
	if err != nil || !p.ResetStreamAt {
		t.Fatal("nonminimal parameter rejected", err)
	}
	p = DefaultTransportParameters()
	p.InitialSourceConnectionID = []byte{1}
	p.ResetStreamAt = true
	encoded, err := p.Marshal(false)
	if err != nil || !bytes.Equal(encoded, []byte{15, 1, 1, 0x1d, 0}) {
		t.Fatal("parameter vector", encoded, err)
	}
}
