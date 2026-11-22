package wire

import (
	"bytes"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"
)

func decodeHex(t testing.TB, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestTransportParameterDefaults(t *testing.T) {
	for _, server := range []bool{false, true} {
		input := []byte{15, 0}
		if server {
			input = append(input, 0, 0)
		}
		p, err := ParseTransportParameters(input, server)
		if err != nil {
			t.Fatal(err)
		}
		if p.InitialSourceConnectionID == nil || len(p.InitialSourceConnectionID) != 0 {
			t.Fatal("lost present empty CID")
		}
		if p.MaxUDPPayloadSize != 65527 || p.ActiveConnectionIDLimit != 2 || p.AckDelayExponent != 3 || p.MaxAckDelay != 25 {
			t.Fatalf("defaults: %+v", p)
		}
		if server && p.OriginalDestinationConnectionID == nil {
			t.Fatal("lost original CID")
		}
		p.AckDelayExponent = 0
		p.MaxAckDelay = 0
		b, err := p.Marshal(server)
		if err != nil {
			t.Fatal(err)
		}
		got, err := ParseTransportParameters(b, server)
		if err != nil || got.AckDelayExponent != 0 || got.MaxAckDelay != 0 {
			t.Fatal("explicit zeros replaced by defaults", err)
		}
	}
}

func TestTransportParameterKnownBytes(t *testing.T) {
	// Independent literal encoding: 8-byte CID, 1200-byte UDP limit, 30-second
	// idle timeout, and a 10000-byte connection flow-control window.
	b := decodeHex(t, "0f083132333435363738030244b001048000753004026710")
	p, err := ParseTransportParameters(b, false)
	if err != nil {
		t.Fatal(err)
	}
	if string(p.InitialSourceConnectionID) != "12345678" || p.MaxUDPPayloadSize != 1200 || p.MaxIdleTimeout != 30000 || p.InitialMaxData != 10000 {
		t.Fatalf("%+v", p)
	}
	// Transport IDs, lengths, and integer values can all use longer encodings.
	p, err = ParseTransportParameters(decodeHex(t, "400f4000400a40024000"), false)
	if err != nil || p.AckDelayExponent != 0 {
		t.Fatal("non-minimal integer encoding", err)
	}
}

func fullParameters() TransportParameters {
	p := DefaultTransportParameters()
	p.OriginalDestinationConnectionID = []byte("original")
	p.InitialSourceConnectionID = []byte("source")
	p.RetrySourceConnectionID = []byte("retry")
	p.StatelessResetToken = &[16]byte{1, 2, 3}
	p.MaxIdleTimeout = MaxVarint
	p.MaxUDPPayloadSize = MaxVarint
	p.InitialMaxData = MaxVarint
	p.InitialMaxStreamDataBidiLocal = 45
	p.InitialMaxStreamDataBidiRemote = 46
	p.InitialMaxStreamDataUni = 47
	p.InitialMaxStreamsBidi = 1 << 60
	p.InitialMaxStreamsUni = 1 << 60
	p.AckDelayExponent = 20
	p.MaxAckDelay = (1 << 14) - 1
	p.DisableActiveMigration = true
	p.ActiveConnectionIDLimit = MaxVarint
	p.MaxDatagramFrameSize = 65535
	p.GreaseQUICBit = true
	p.PreferredAddress = &PreferredAddress{IPv4: [4]byte{192, 0, 2, 1}, IPv4Port: 443, IPv6: [16]byte{0x20, 1, 0xd, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}, IPv6Port: 8443, ConnectionID: []byte("preferred"), StatelessResetToken: [16]byte{15, 14}}
	return p
}

func TestTransportParameterRoundTripAndOwnership(t *testing.T) {
	want := fullParameters()
	b, err := want.Marshal(true)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseTransportParameters(b, true)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
	clear(b)
	if !reflect.DeepEqual(got, want) {
		t.Fatal("parameters alias input buffer")
	}
}

func TestInvalidTransportParameters(t *testing.T) {
	for _, tc := range []struct {
		name   string
		server bool
		b      []byte
	}{
		{"missing client CID", false, nil}, {"missing server original CID", true, []byte{15, 0}},
		{"truncated ID", false, []byte{0xc0}}, {"truncated length", false, []byte{15, 0x40}},
		{"truncated value", false, []byte{15, 4, 1}}, {"empty integer", false, []byte{15, 0, 4, 0}},
		{"integer trailing byte", false, []byte{15, 0, 4, 2, 1, 0}},
		{"duplicate known", false, []byte{15, 0, 0x40, 15, 0}},
		{"duplicate unknown", false, []byte{15, 0, 31, 0, 31, 0}},
		{"nonempty disable migration", false, []byte{15, 0, 12, 1, 0}},
		{"nonempty grease", false, []byte{15, 0, 0x6a, 0xb2, 1, 0}},
		{"bad reset token", true, []byte{15, 0, 0, 0, 2, 1, 0}},
		{"empty preferred address", true, []byte{15, 0, 0, 0, 13, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseTransportParameters(tc.b, tc.server); !errors.Is(err, ErrTransportParameter) {
				t.Fatal("expected parameter error", err)
			}
		})
	}
	for _, id := range []uint64{0, 2, 13, 16} {
		b := appendParameter([]byte{15, 0}, id, nil)
		if _, err := ParseTransportParameters(b, false); !errors.Is(err, ErrTransportParameter) {
			t.Fatalf("client sent server-only 0x%x: %v", id, err)
		}
	}
	for _, tc := range []struct{ id, v uint64 }{{3, 1199}, {8, (1 << 60) + 1}, {9, (1 << 60) + 1}, {10, 21}, {11, 1 << 14}, {14, 0}, {14, 1}} {
		value, _ := AppendVarint(nil, tc.v)
		b := appendParameter([]byte{15, 0}, tc.id, value)
		_, err := ParseTransportParameters(b, false)
		var pe *ParameterError
		if !errors.As(err, &pe) || pe.ID != tc.id {
			t.Fatalf("id %d: %v", tc.id, err)
		}
	}
	for _, id := range []uint64{0, 15, 16} {
		b := []byte{15, 0, 0, 0}
		if id == 15 {
			b = []byte{0, 0}
		}
		if id == 0 {
			b = []byte{15, 0}
		}
		b = appendParameter(b, id, make([]byte, 21))
		if _, err := ParseTransportParameters(b, true); err == nil {
			t.Fatalf("oversized CID %d", id)
		}
	}
	if _, err := ParseTransportParameters(make([]byte, 65536), false); err == nil {
		t.Fatal("oversized extension")
	}
	// Unknown parameters must be ignored even when their bytes are not varints.
	b := appendParameter([]byte{15, 0}, 173, []byte{0xff})
	if _, err := ParseTransportParameters(b, false); err != nil {
		t.Fatal("unknown parameter", err)
	}
}

func TestInvalidPreferredAddress(t *testing.T) {
	for _, cidLen := range []int{0, 21} {
		value := make([]byte, 41+cidLen)
		value[24] = byte(cidLen)
		b := appendParameter([]byte{15, 1, 1, 0, 0}, 13, value)
		if _, err := ParseTransportParameters(b, true); err == nil {
			t.Fatal("invalid preferred CID")
		}
	}
	p := fullParameters()
	p.InitialSourceConnectionID = []byte{}
	if _, err := p.Marshal(true); err == nil {
		t.Fatal("preferred address with zero-length initial CID")
	}
	p = fullParameters()
	p.PreferredAddress.ConnectionID = nil
	if _, err := p.Marshal(true); err == nil {
		t.Fatal("empty preferred CID")
	}
}

func TestConnectionIDBinding(t *testing.T) {
	p := fullParameters()
	c := ConnectionIDContext{PeerIsServer: true, InitialSourceConnectionID: []byte("source"), OriginalDestinationConnectionID: []byte("original"), RetryUsed: true, RetrySourceConnectionID: []byte("retry")}
	if err := p.ValidateConnectionIDs(c); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*ConnectionIDContext){
		func(c *ConnectionIDContext) { c.InitialSourceConnectionID = []byte("wrong") },
		func(c *ConnectionIDContext) { c.OriginalDestinationConnectionID = []byte("wrong") },
		func(c *ConnectionIDContext) { c.RetrySourceConnectionID = []byte("wrong") },
		func(c *ConnectionIDContext) { c.RetryUsed = false },
	} {
		bad := c
		mutate(&bad)
		if err := p.ValidateConnectionIDs(bad); err == nil {
			t.Fatal("accepted CID mismatch")
		}
	}
	p.RetrySourceConnectionID = nil
	if err := p.ValidateConnectionIDs(c); err == nil {
		t.Fatal("missing Retry CID")
	}
	c.RetryUsed = false
	if err := p.ValidateConnectionIDs(c); err != nil {
		t.Fatal(err)
	}
	p.RetrySourceConnectionID = []byte{}
	c.RetryUsed = true
	c.RetrySourceConnectionID = []byte{}
	if err := p.ValidateConnectionIDs(c); err != nil {
		t.Fatal("present empty Retry CID", err)
	}
	p = DefaultTransportParameters()
	p.InitialSourceConnectionID = []byte{}
	if err := p.ValidateConnectionIDs(ConnectionIDContext{}); err != nil {
		t.Fatal("zero-length client CID", err)
	}
}

func TestMarshalRejectsInvalidParameters(t *testing.T) {
	p := fullParameters()
	p.MaxIdleTimeout = MaxVarint + 1
	if _, err := p.Marshal(true); err == nil {
		t.Fatal("integer overflow")
	}
	p = fullParameters()
	if _, err := p.Marshal(false); err == nil {
		t.Fatal("server-only parameters")
	}
}

func FuzzTransportParameters(f *testing.F) {
	f.Add([]byte{15, 1, 1, 0x3e, 1, 0}, false)
	f.Add([]byte{15, 1, 1, 0x3e, 8, 0xc0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff}, false)
	f.Add([]byte{15, 0}, false)
	f.Add([]byte{15, 0, 0, 0}, true)
	p := fullParameters()
	b, _ := p.Marshal(true)
	f.Add(b, true)
	f.Fuzz(func(t *testing.T, b []byte, server bool) {
		original := bytes.Clone(b)
		p, err := ParseTransportParameters(b, server)
		if !bytes.Equal(b, original) {
			t.Fatal("mutated input")
		}
		if err != nil {
			return
		}
		encoded, err := p.Marshal(server)
		if err != nil {
			t.Fatal(err)
		}
		again, err := ParseTransportParameters(encoded, server)
		if err != nil || !reflect.DeepEqual(p, again) {
			t.Fatal("round trip", err)
		}
	})
}
