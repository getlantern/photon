package token

import (
	"bytes"
	"errors"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestRetryTokens(t *testing.T) {
	now := time.Unix(1000, 0)
	addr := netip.MustParseAddrPort("192.0.2.1:4433")
	original := []byte("original")
	retry := []byte("retrycid")
	old := [32]byte{1}
	newKey := [32]byte{2}
	issuer, _ := NewRetryTokens(old, nil, 30*time.Second)
	rotated, _ := NewRetryTokens(newKey, &old, 30*time.Second)
	retired, _ := NewRetryTokens(newKey, nil, 30*time.Second)
	longer, _ := NewRetryTokens(newKey, &old, time.Minute)
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		raw, err := issuer.Issue(addr, v, original, retry, now)
		if err != nil {
			t.Fatal(err)
		}
		for _, decoder := range []*RetryTokens{issuer, rotated} {
			got, err := decoder.Validate(raw, addr, v, retry, now.Add(29*time.Second))
			if err != nil || !bytes.Equal(got, original) {
				t.Fatal(got, err)
			}
			got[0] ^= 1
			again, err := decoder.Validate(raw, addr, v, retry, now)
			if err != nil || !bytes.Equal(again, original) {
				t.Fatal("aliased token")
			}
		}
		if _, err := retired.Validate(raw, addr, v, retry, now); err == nil {
			t.Fatal("retired key accepted")
		}
		for i := range raw {
			bad := bytes.Clone(raw)
			bad[i] ^= 1
			if _, err := issuer.Validate(bad, addr, v, retry, now); !errors.Is(err, ErrRetryToken) || errors.Is(err, ErrReadableRetryToken) {
				t.Fatal("tampering accepted", i)
			}
		}
		if _, err := longer.Validate(raw, addr, v, retry, now.Add(30*time.Second)); err == nil {
			t.Fatal("rotation extended issued lifetime")
		}
		for _, at := range []time.Time{now.Add(-time.Second), now.Add(30 * time.Second)} {
			if _, err := issuer.Validate(raw, addr, v, retry, at); !errors.Is(err, ErrReadableRetryToken) || !errors.Is(err, ErrRetryToken) {
				t.Fatal("invalid lifetime")
			}
		}
		for _, other := range []netip.AddrPort{netip.MustParseAddrPort("192.0.2.2:4433"), netip.MustParseAddrPort("192.0.2.1:4434")} {
			if _, err := issuer.Validate(raw, other, v, retry, now); err == nil {
				t.Fatal("address not bound")
			}
		}
		otherVersion := wire.Version1
		if v == otherVersion {
			otherVersion = wire.Version2
		}
		if _, err := issuer.Validate(raw, addr, otherVersion, retry, now); err == nil {
			t.Fatal("version not bound")
		}
		if _, err := issuer.Validate(raw, addr, v, []byte("othercid"), now); err == nil {
			t.Fatal("Retry CID not bound")
		}
		mapped := netip.MustParseAddrPort("[::ffff:192.0.2.1]:4433")
		if _, err := issuer.Validate(raw, mapped, v, retry, now); err != nil {
			t.Fatal("IPv4 mapping", err)
		}
	}
}

func TestRetryTokenConcurrentAndEmptyCID(t *testing.T) {
	codec, _ := NewRetryTokens([32]byte{4}, nil, time.Minute)
	addr := netip.MustParseAddrPort("[2001:db8::1]:1234")
	now := time.Unix(1000, 0)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Go(func() {
			raw, err := codec.Issue(addr, wire.Version2, []byte("original"), nil, now)
			if err != nil {
				t.Error(err)
				return
			}
			if _, err := codec.Validate(raw, addr, wire.Version2, nil, now); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
}

func FuzzRetryTokenValidation(f *testing.F) {
	codec, _ := NewRetryTokens([32]byte{7}, nil, time.Minute)
	addr := netip.MustParseAddrPort("192.0.2.1:1234")
	now := time.Unix(1000, 0)
	valid, _ := codec.Issue(addr, wire.Version1, []byte("original"), []byte("retrycid"), now)
	f.Add(valid)
	f.Add([]byte{})
	f.Add([]byte("invalid"))
	f.Fuzz(func(t *testing.T, b []byte) {
		original, err := codec.Validate(b, addr, wire.Version1, []byte("retrycid"), now)
		if err == nil && !bytes.Equal(original, []byte("original")) {
			t.Fatal("unauthenticated context accepted")
		}
	})
}
