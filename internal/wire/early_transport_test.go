package wire

import (
	"bytes"
	"testing"
)

func TestEarlyTransport(t *testing.T) {
	p := DefaultTransportParameters()
	p.MaxIdleTimeout = 12345
	p.MaxUDPPayloadSize = 1452
	p.DisableActiveMigration = true
	p.ResetStreamAt = true
	p.InitialMaxData = 8192
	expected := p.EarlyTransport()
	// None of these connection-specific values may enter the snapshot.
	p.InitialSourceConnectionID = []byte("new cid")
	p.OriginalDestinationConnectionID = []byte("old cid")
	p.RetrySourceConnectionID = []byte("retry")
	p.StatelessResetToken = &[16]byte{1}
	p.PreferredAddress = &PreferredAddress{ConnectionID: []byte("preferred")}
	p.AckDelayExponent = 10
	p.MaxAckDelay = 99
	path := uint64(4)
	p.InitialMaxPathID = &path
	if p.EarlyTransport() != expected {
		t.Fatal("connection-specific state retained")
	}
	for _, s := range []EarlyTransport{expected, DefaultTransportParameters().EarlyTransport(), {
		EarlyLimits:    EarlyLimits{MaxVarint, MaxVarint, MaxVarint, MaxVarint, MaxVarint, 1 << 60, 1 << 60, MaxVarint},
		MaxIdleTimeout: MaxVarint, MaxUDPPayloadSize: MaxVarint, DisableActiveMigration: true,
	}} {
		b, err := s.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		copyBefore := bytes.Clone(b)
		got, err := ParseEarlyTransport(b)
		if err != nil || got != s || !bytes.Equal(b, copyBefore) {
			t.Fatal("round trip", got, err)
		}
		for n := range len(b) {
			if _, err := ParseEarlyTransport(b[:n]); err == nil {
				t.Fatal("truncation", n)
			}
		}
		if _, err := ParseEarlyTransport(append(bytes.Clone(b), 0)); err == nil {
			t.Fatal("trailing byte")
		}
		for _, index := range []int{0, 1, len(b) - 2, len(b) - 1} {
			bad := bytes.Clone(b)
			bad[index] = 255
			if _, err := ParseEarlyTransport(bad); err == nil {
				t.Fatal("invalid schema, size or flag", index)
			}
		}
	}
	for _, mutate := range []func(*EarlyTransport){
		func(s *EarlyTransport) { s.MaxIdleTimeout = MaxVarint + 1 },
		func(s *EarlyTransport) { s.MaxUDPPayloadSize = 1199 },
		func(s *EarlyTransport) { s.MaxUDPPayloadSize = MaxVarint + 1 },
		func(s *EarlyTransport) { s.ActiveConnectionIDLimit = 1 },
	} {
		bad := expected
		mutate(&bad)
		if _, err := bad.Marshal(); err == nil {
			t.Fatal("invalid snapshot encoded")
		}
	}
	old, _ := expected.EarlyLimits.Marshal()
	if _, err := ParseEarlyTransport(old); err == nil {
		t.Fatal("credit-only schema accepted")
	}
}

func FuzzEarlyTransport(f *testing.F) {
	b, _ := DefaultTransportParameters().EarlyTransport().Marshal()
	f.Add(b)
	withReset := DefaultTransportParameters().EarlyTransport()
	withReset.ResetStreamAt = true
	b, _ = withReset.Marshal()
	f.Add(b)
	f.Add([]byte{2, 9, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0x80, 0, 0xff, 0xf7, 0})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		s, err := ParseEarlyTransport(b)
		if err != nil {
			return
		}
		encoded, err := s.Marshal()
		if err != nil || len(encoded) > 85 {
			t.Fatal("invalid parsed state", err)
		}
		again, err := ParseEarlyTransport(encoded)
		if err != nil || again != s {
			t.Fatal("unstable round trip", err)
		}
	})
}

func TestEarlyTransportResetCompatibility(t *testing.T) {
	without := DefaultTransportParameters().EarlyTransport()
	with := without
	with.ResetStreamAt = true
	if !with.Supports(with) || !with.Supports(without) || !without.Supports(without) || without.Supports(with) {
		t.Fatal("reset capability compatibility")
	}
	// Actual schema-2 bytes, independent of the current encoder: embedded
	// credit schema, idle timeout, max UDP payload (65527), migration flag.
	legacy := []byte{2, 9, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0x80, 0, 0xff, 0xf7, 0}
	got, err := ParseEarlyTransport(legacy)
	if err != nil || got != without || got.ResetStreamAt {
		t.Fatal("legacy state", got, err)
	}
	if !with.Supports(got) || !without.Supports(got) {
		t.Fatal("legacy ticket rejected")
	}
	bad := with
	bad.MaxUDPPayloadSize = 0
	if bad.Supports(with) || with.Supports(bad) {
		t.Fatal("invalid state accepted")
	}
}
