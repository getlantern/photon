package wire

import (
	"bytes"
	"testing"
)

func TestEarlyLimitsCreditComparison(t *testing.T) {
	old := EarlyLimits{4, 100, 101, 102, 103, 10, 11, 1200}
	fields := func(s *EarlyLimits) []*uint64 {
		return []*uint64{&s.ActiveConnectionIDLimit, &s.InitialMaxData, &s.InitialMaxStreamDataBidiLocal, &s.InitialMaxStreamDataBidiRemote, &s.InitialMaxStreamDataUni, &s.InitialMaxStreamsBidi, &s.InitialMaxStreamsUni, &s.MaxDatagramFrameSize}
	}
	if !old.Supports(old) {
		t.Fatal("equal credit rejected")
	}
	for i := 0; i < 8; i++ {
		lower, higher := old, old
		*fields(&lower)[i]--
		*fields(&higher)[i]++
		if lower.Supports(old) {
			t.Fatalf("reduced credit %d accepted", i)
		}
		if !higher.Supports(old) {
			t.Fatalf("increased credit %d rejected", i)
		}
	}
	disabled := old
	disabled.MaxDatagramFrameSize = 0
	if disabled.Supports(old) || !old.Supports(disabled) {
		t.Fatal("DATAGRAM support change")
	}
	for _, bad := range []EarlyLimits{{}, {ActiveConnectionIDLimit: 1}, {ActiveConnectionIDLimit: 2, InitialMaxStreamsBidi: 1<<60 + 1}, {ActiveConnectionIDLimit: 2, InitialMaxData: MaxVarint + 1}} {
		if bad.Supports(bad) {
			t.Fatal("invalid self-comparison")
		}
		if _, err := bad.Marshal(); err == nil {
			t.Fatal("invalid encoding accepted")
		}
	}
}

func TestEarlyLimitsEncoding(t *testing.T) {
	defaults := DefaultTransportParameters().EarlyLimits()
	encoded, err := defaults.Marshal()
	if err != nil || !bytes.Equal(encoded, []byte{1, 2, 0, 0, 0, 0, 0, 0, 0}) {
		t.Fatalf("default schema: %x %v", encoded, err)
	}
	for _, s := range []EarlyLimits{defaults, {MaxVarint, MaxVarint, MaxVarint, MaxVarint, MaxVarint, 1 << 60, 1 << 60, MaxVarint}} {
		b, err := s.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := ParseEarlyLimits(b)
		if err != nil || parsed != s {
			t.Fatal("round trip", err)
		}
		for i := 0; i < len(b); i++ {
			if _, err := ParseEarlyLimits(b[:i]); err == nil {
				t.Fatalf("truncation %d accepted", i)
			}
		}
		if _, err := ParseEarlyLimits(append(bytes.Clone(b), 0)); err == nil {
			t.Fatal("trailing bytes accepted")
		}
		b[0] = 2
		if _, err := ParseEarlyLimits(b); err == nil {
			t.Fatal("unknown schema accepted")
		}
	}
}

func TestEarlyLimitsExcludeConnectionSpecificState(t *testing.T) {
	p := DefaultTransportParameters()
	p.InitialMaxData = 77
	saved := p.EarlyLimits()
	path := uint64(3)
	p.InitialSourceConnectionID = []byte("new source")
	p.OriginalDestinationConnectionID = []byte("old destination")
	p.RetrySourceConnectionID = []byte("retry")
	p.StatelessResetToken = &[16]byte{1}
	p.PreferredAddress = &PreferredAddress{}
	p.InitialMaxPathID = &path
	p.AckDelayExponent = 9
	p.MaxAckDelay = 99
	if p.EarlyLimits() != saved {
		t.Fatal("unrelated state changed limit snapshot")
	}
	p.InitialMaxData++
	if saved.InitialMaxData != 77 {
		t.Fatal("snapshot aliases source")
	}
}

func FuzzEarlyLimits(f *testing.F) {
	f.Add([]byte{1, 2, 0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{2})
	f.Fuzz(func(t *testing.T, b []byte) {
		s, err := ParseEarlyLimits(b)
		if err != nil {
			return
		}
		encoded, err := s.Marshal()
		if err != nil || len(encoded) > 65 {
			t.Fatal("accepted state cannot encode", err)
		}
		again, err := ParseEarlyLimits(encoded)
		if err != nil || again != s || !s.Supports(s) {
			t.Fatal("unstable valid state", err)
		}
	})
}
