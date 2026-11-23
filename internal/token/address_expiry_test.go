package token

import (
	"encoding/binary"
	"math"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestAddressTokenPreciseExpiry(t *testing.T) {
	peer := netip.MustParseAddrPort("192.0.2.1:1234")
	now := time.Unix(100000, 999999999)
	for _, lifetime := range []time.Duration{1, 500 * time.Millisecond, time.Second + 1, 30 * 24 * time.Hour, math.MaxInt64} {
		codec, err := NewAddressTokens([32]byte{1}, nil, lifetime)
		if err != nil {
			t.Fatal(err)
		}
		b, err := codec.Issue(peer, wire.Version1, now)
		if err != nil {
			t.Fatal(err)
		}
		for _, offset := range []time.Duration{-1, 0, lifetime - 1, lifetime} {
			valid := offset >= 0 && offset < lifetime
			if got := codec.Validate(b, peer, wire.Version1, now.Add(offset)) == nil; got != valid {
				t.Fatalf("lifetime %v offset %v valid %v", lifetime, offset, got)
			}
		}
		shorter, _ := NewAddressTokens([32]byte{1}, nil, time.Nanosecond)
		if shorter.Validate(b, peer, wire.Version1, now.Add(1)) == nil {
			t.Fatal("validator age limit ignored")
		}
	}
}

func TestAddressTokenLegacyAndMalformedTimes(t *testing.T) {
	codec, _ := NewAddressTokens([32]byte{1}, nil, time.Hour)
	peer := netip.MustParseAddrPort("192.0.2.1:1234")
	now := time.Unix(1000, 0)
	aad, _ := addressContext(peer, wire.Version1)
	seal := func(schema byte, plain []byte) []byte {
		header := make([]byte, 26)
		header[0], header[1] = 0x4e, schema
		return codec.keys[0].Seal(header, header[2:], plain, aad)
	}
	plain := binary.BigEndian.AppendUint64(nil, 1000)
	plain = binary.BigEndian.AppendUint32(plain, 60)
	legacy := seal(1, plain)
	if codec.Validate(legacy, peer, wire.Version1, now.Add(60*time.Second-1)) != nil || codec.Validate(legacy, peer, wire.Version1, now.Add(60*time.Second)) == nil {
		t.Fatal("legacy expiry")
	}
	for _, mode := range []string{"seconds", "nanos", "duration", "zero", "reserved", "schema"} {
		p := make([]byte, 32)
		binary.BigEndian.PutUint64(p, 1000)
		binary.BigEndian.PutUint64(p[12:], uint64(time.Hour))
		schema := byte(2)
		switch mode {
		case "seconds":
			binary.BigEndian.PutUint64(p, math.MaxUint64)
		case "nanos":
			binary.BigEndian.PutUint32(p[8:], 1e9)
		case "duration":
			binary.BigEndian.PutUint64(p[12:], math.MaxUint64)
		case "zero":
			binary.BigEndian.PutUint64(p[12:], 0)
		case "reserved":
			p[31] = 1
		case "schema":
			schema = 1
		}
		if codec.Validate(seal(schema, p), peer, wire.Version1, now) == nil {
			t.Fatal("accepted malformed", mode)
		}
	}
}
