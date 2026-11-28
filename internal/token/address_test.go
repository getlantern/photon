package token

import (
	"bytes"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// A version 3 token carries its resume ID, sealed: two tokens for one ID
// differ, tampering fails, and flipping the format byte between versions 2
// and 3 fails either way.
func TestAddressTokenResumeID(t *testing.T) {
	tokens, err := NewAddressTokens([32]byte{1}, nil, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	address := netip.MustParseAddrPort("192.0.2.1:4433")
	now := time.Unix(1000, 0)
	id := ResumeID{1, 2, 3, 4, 5, 6, 7, 8}
	token, err := tokens.IssueResume(address, wire.Version1, now, id)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := tokens.ValidateResume(token, address, wire.Version1, now)
	if err != nil || !ok || got != id {
		t.Fatal("resume ID", got, ok, err)
	}
	if again, _ := tokens.IssueResume(address, wire.Version1, now, id); bytes.Equal(again, token) {
		t.Fatal("two tokens for one ID are equal")
	}
	for i := range token {
		bad := bytes.Clone(token)
		bad[i] ^= 1
		if _, _, err := tokens.ValidateResume(bad, address, wire.Version1, now); err == nil {
			t.Fatal("tamper", i)
		}
	}
	plain, _ := tokens.Issue(address, wire.Version1, now)
	if _, ok, err := tokens.ValidateResume(plain, address, wire.Version1, now); err != nil || ok {
		t.Fatal("version 2 token", ok, err)
	}
	for _, tc := range []struct {
		token  []byte
		format byte
	}{{plain, 3}, {token, 2}} {
		bad := bytes.Clone(tc.token)
		bad[1] = tc.format
		if _, _, err := tokens.ValidateResume(bad, address, wire.Version1, now); err == nil {
			t.Fatal("format flipped to", tc.format)
		}
	}
}

func TestAddressTokenBindingAndRotation(t *testing.T) {
	var oldKey, newKey [32]byte
	oldKey[0] = 1
	newKey[0] = 2
	old, _ := NewAddressTokens(oldKey, nil, time.Hour)
	overlap, _ := NewAddressTokens(newKey, &oldKey, time.Hour)
	retired, _ := NewAddressTokens(newKey, nil, time.Hour)
	shortened, _ := NewAddressTokens(oldKey, nil, 10*time.Second)
	now := time.Unix(100000, 0)
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, ip := range []string{"192.0.2.1", "2001:db8::1"} {
			address := netip.AddrPortFrom(netip.MustParseAddr(ip), 1234)
			token, err := old.Issue(address, v, now)
			if err != nil {
				t.Fatal(err)
			}
			before := bytes.Clone(token)
			for _, port := range []uint16{1234, 5678} {
				if err := overlap.Validate(token, netip.AddrPortFrom(address.Addr(), port), v, now.Add(time.Second)); err != nil {
					t.Fatal("valid address", err)
				}
			}
			if !bytes.Equal(token, before) {
				t.Fatal("modified input")
			}
			if retired.Validate(token, address, v, now) == nil {
				t.Fatal("retired key accepted")
			}
			if shortened.Validate(token, address, v, now.Add(10*time.Second)) == nil {
				t.Fatal("current expiry policy ignored")
			}
			if old.Validate(token, address, v, now.Add(time.Hour)) == nil || old.Validate(token, address, v, now.Add(-time.Second)) == nil {
				t.Fatal("invalid time accepted")
			}
			other := wire.Version1
			if other == v {
				other = wire.Version2
			}
			if old.Validate(token, address, other, now) == nil {
				t.Fatal("cross-version token")
			}
			if old.Validate(token, netip.MustParseAddrPort("192.0.2.2:1234"), v, now) == nil {
				t.Fatal("wrong IP")
			}
			if ip == "192.0.2.1" && old.Validate(token, netip.MustParseAddrPort("[::ffff:192.0.2.1]:7777"), v, now) != nil {
				t.Fatal("mapped IPv4 mismatch")
			}
			for n := range len(token) {
				if old.Validate(token[:n], address, v, now) == nil {
					t.Fatal("truncation", n)
				}
			}
			for i := range token {
				bad := bytes.Clone(token)
				bad[i] ^= 1
				if old.Validate(bad, address, v, now) == nil {
					t.Fatal("tamper", i)
				}
			}
			if old.Validate(append(bytes.Clone(token), 0), address, v, now) == nil {
				t.Fatal("trailing byte")
			}
			again, _ := old.Issue(address, v, now)
			if bytes.Equal(again, token) {
				t.Fatal("nonce reused")
			}
			retry, _ := NewRetryTokens(oldKey, nil, time.Minute)
			r, err := retry.Issue(address, v, []byte("original"), []byte("retrycid"), now)
			if err != nil {
				t.Fatal(err)
			}
			if old.Validate(r, address, v, now) == nil {
				t.Fatal("Retry accepted as NEW_TOKEN")
			}
			if _, err := retry.Validate(token, address, v, []byte("retrycid"), now); err == nil {
				t.Fatal("NEW_TOKEN accepted as Retry")
			}
		}
	}
}

func TestAddressTokenInvalidInputs(t *testing.T) {
	for _, duration := range []time.Duration{0, -time.Nanosecond} {
		if _, err := NewAddressTokens([32]byte{}, nil, duration); err == nil {
			t.Fatal("invalid lifetime", duration)
		}
	}
	tokens, _ := NewAddressTokens([32]byte{}, nil, time.Minute)
	for _, address := range []netip.AddrPort{{}, netip.MustParseAddrPort("192.0.2.1:0")} {
		if _, err := tokens.Issue(address, wire.Version1, time.Unix(100, 0)); err == nil {
			t.Fatal("invalid address")
		}
	}
	address := netip.MustParseAddrPort("192.0.2.1:1234")
	if _, err := tokens.Issue(address, wire.Version(42), time.Unix(100, 0)); err == nil {
		t.Fatal("unsupported version")
	}
	if _, err := tokens.Issue(address, wire.Version1, time.Unix(-1, 0)); err == nil {
		t.Fatal("negative clock")
	}
}

func FuzzAddressToken(f *testing.F) {
	tokens, _ := NewAddressTokens([32]byte{7}, nil, time.Hour)
	address := netip.MustParseAddrPort("192.0.2.1:1234")
	now := time.Unix(100000, 0)
	valid, _ := tokens.Issue(address, wire.Version1, now)
	f.Add(valid)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		before := bytes.Clone(b)
		err := tokens.Validate(b, address, wire.Version1, now)
		if !bytes.Equal(before, b) {
			t.Fatal("modified input")
		}
		if err == nil && (tokens.Validate(b, address, wire.Version2, now) == nil || tokens.Validate(b, address, wire.Version1, now.Add(time.Hour)) == nil) {
			t.Fatal("binding bypass")
		}
	})
}

// Admission must classify damaged format bytes before validation. These sizes
// must stay disjoint from Retry envelopes if either codec changes.
func TestAddressTokenClassification(t *testing.T) {
	for size := 0; size <= 512; size++ {
		b := make([]byte, size)
		if got := IsAddressToken(b); got != (size == 54 || size == 74) {
			t.Fatalf("size %d classified %v", size, got)
		}
	}
	address := netip.MustParseAddrPort("192.0.2.1:1234")
	retries, err := NewRetryTokens([32]byte{9}, nil, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for size := 8; size <= 20; size++ {
		opaque, err := retries.Issue(address, wire.Version1, make([]byte, size), []byte{1, 2, 3, 4, 5, 6, 7, 8}, time.Unix(1000, 0))
		if err != nil {
			t.Fatal(err)
		}
		if IsAddressToken(opaque) {
			t.Fatalf("Retry with %d-byte CID misclassified", size)
		}
	}
}
