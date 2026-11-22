package qpack

import (
	"sync"
	"testing"
)

func TestStaticRFCEntries(t *testing.T) {
	if len(staticTable) != StaticTableLen {
		t.Fatal("wrong static table length")
	}
	// Boundary entries, pseudo fields, exact punctuation and case-sensitive values
	// from RFC 9204 Appendix A, independently selected from the published table.
	for _, tc := range []struct {
		id   uint64
		want Field
	}{
		{0, Field{":authority", ""}}, {1, Field{":path", "/"}},
		{17, Field{":method", "GET"}}, {25, Field{":status", "200"}},
		{31, Field{"accept-encoding", "gzip, deflate, br"}},
		{52, Field{"content-type", "text/html; charset=utf-8"}},
		{54, Field{"content-type", "text/plain;charset=utf-8"}},
		{58, Field{"strict-transport-security", "max-age=31536000; includesubdomains; preload"}},
		{63, Field{":status", "100"}}, {73, Field{"access-control-allow-credentials", "FALSE"}},
		{74, Field{"access-control-allow-credentials", "TRUE"}},
		{85, Field{"content-security-policy", "script-src 'none'; object-src 'none'; base-uri 'none'"}},
		{98, Field{"x-frame-options", "sameorigin"}},
	} {
		got, err := Static(tc.id)
		if err != nil || got != tc.want {
			t.Fatalf("index %d: %v %v", tc.id, got, err)
		}
	}
	for _, id := range []uint64{99, MaxInteger, ^uint64(0)} {
		if _, err := Static(id); err != ErrStaticIndex {
			t.Fatal(id, err)
		}
	}
}
func TestStaticSearch(t *testing.T) {
	for i, f := range staticTable {
		id, exact, found := FindStatic(f)
		if !found || !exact || id != uint64(i) {
			t.Fatalf("%d: %d %t %t", i, id, exact, found)
		}
	}
	for _, tc := range []struct {
		field Field
		id    uint64
		found bool
	}{
		{Field{":method", "PATCH"}, 15, true},
		{Field{"content-type", "application/example"}, 44, true},
		{Field{"access-control-allow-headers", "different"}, 33, true},
		{Field{"x-frame-options", "DENY"}, 97, true},
		{Field{"Content-Type", "application/json"}, 0, false},
		{Field{"x-independent", ""}, 0, false},
	} {
		id, exact, found := FindStatic(tc.field)
		if exact || found != tc.found || id != tc.id {
			t.Fatalf("%v: %d %t %t", tc.field, id, exact, found)
		}
	}
	// Returned fields do not expose writable storage in the lookup table.
	f, _ := Static(0)
	f.Name = "changed"
	again, _ := Static(0)
	if again.Name == f.Name {
		t.Fatal("mutated static table")
	}
}
func TestConcurrentStaticLookup(t *testing.T) {
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := uint64(0); i < StaticTableLen; i++ {
				f, err := Static(i)
				if err != nil {
					t.Error(err)
					return
				}
				id, exact, found := FindStatic(f)
				if id != i || !exact || !found {
					t.Error("lookup changed")
				}
			}
		}()
	}
	wg.Wait()
}
