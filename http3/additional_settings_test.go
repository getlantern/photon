package http3

import (
	"context"
	"crypto/tls"
	"net/http"
	"testing"

	"github.com/getlantern/photon/internal/http3conn"
	"github.com/getlantern/photon/internal/wire"
)

func TestAdditionalSettingsValidationAndOwnership(t *testing.T) {
	invalid := []map[uint64]uint64{{wire.MaxVarint + 1: 0}, {0xface: wire.MaxVarint + 1}}
	for id := uint64(1); id <= 8; id++ {
		invalid = append(invalid, map[uint64]uint64{id: 0})
	}
	invalid = append(invalid, map[uint64]uint64{0x33: 1})
	large := make(map[uint64]uint64)
	for i := uint64(0); i < 60; i++ {
		large[0x100+i] = i
	}
	invalid = append(invalid, large)
	for _, extra := range invalid {
		if _, err := clientOptions(&ClientConfig{AdditionalSettings: extra}); err == nil {
			t.Fatal("client accepted", extra)
		}
		s := &Server{AdditionalSettings: extra}
		if _, err := s.ConfigureTLSConfig(&tls.Config{}); err == nil {
			t.Fatal("ticket policy accepted", extra)
		}
		if err := s.ServeQUICConn(context.Background(), nil); err == nil || err.Error() == "http3: nil serving context or connection" {
			t.Fatal("server failed to validate before ownership", err)
		}
		request, _ := http.NewRequest("GET", "https://127.0.0.1:1", nil)
		transport := &Transport{AdditionalSettings: extra}
		if _, err := transport.RoundTrip(request); err == nil {
			t.Fatal("transport accepted", extra)
		}
		if len(transport.all) != 0 || len(transport.pool) != 0 {
			t.Fatal("invalid settings started dial")
		}
	}
	delete(large, 0x100+59)
	fit := &Server{AdditionalSettings: large, EnableDatagrams: true, ExtendedConnectHandlers: map[string]http.Handler{"test": http.NotFoundHandler()}}
	if _, _, _, err := fit.limits(); err != nil {
		t.Fatal("59-entry config rejected", err)
	}
	if count := len(fit.settings(65536)); count != 64 {
		t.Fatal("full settings budget", count)
	}
	left, right := map[uint64]uint64{}, map[uint64]uint64{}
	for id := uint64(0); id < 30; id++ {
		left[0x100+id] = 0
		right[0x200+id] = 0
	}
	if _, err := (&Transport{AdditionalSettings: left, ClientConfig: &ClientConfig{AdditionalSettings: right}}).clientOptions(); err == nil {
		t.Fatal("merged settings exceed budget")
	}
	source := map[uint64]uint64{0xface: wire.MaxVarint, 0: 0}
	cfg, err := clientOptions(&ClientConfig{AdditionalSettings: source})
	if err != nil {
		t.Fatal(err)
	}
	source[0xface] = 0
	if cfg.AdditionalSettings[0xface] != wire.MaxVarint {
		t.Fatal("map not copied")
	}
	tr := &Transport{AdditionalSettings: map[uint64]uint64{0x21: 3}, ClientConfig: &cfg}
	merged, err := tr.clientOptions()
	if err != nil || len(merged.AdditionalSettings) != 3 {
		t.Fatal(merged, err)
	}
	merged.AdditionalSettings[0] = 99
	if cfg.AdditionalSettings[0] != 0 {
		t.Fatal("merge mutated client configuration")
	}
	tr.AdditionalSettings = map[uint64]uint64{0: 0}
	if _, err := tr.clientOptions(); err == nil {
		t.Fatal("ambiguous duplicate accepted")
	}
}

func TestAdditionalSettingsTicketBinding(t *testing.T) {
	_, original, err := cloneHTTPSession(cacheTestTicket(t))
	if err != nil {
		t.Fatal(err)
	}
	original.EarlyData = true
	var issued *tls.SessionState
	source := &tls.Config{
		WrapSession:   func(_ tls.ConnectionState, s *tls.SessionState) ([]byte, error) { issued = s; return []byte("id"), nil },
		UnwrapSession: func([]byte, tls.ConnectionState) (*tls.SessionState, error) { return issued, nil },
	}
	seed := &Server{AdditionalSettings: map[uint64]uint64{0xface: 10, 0x21: 20}}
	config, err := seed.ConfigureTLSConfig(source)
	if err != nil {
		t.Fatal(err)
	}
	id, err := config.WrapSession(tls.ConnectionState{}, original)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		extra  map[uint64]uint64
		accept bool
	}{
		{"same", map[uint64]uint64{0x21: 20, 0xface: 10}, true},
		{"changed", map[uint64]uint64{0x21: 20, 0xface: 11}, false},
		{"removed", map[uint64]uint64{0x21: 20}, false},
		{"added", map[uint64]uint64{0x21: 20, 0xface: 10, 0x22: 0}, false},
		{"none", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current, err := (&Server{AdditionalSettings: tc.extra}).ConfigureTLSConfig(source)
			if err != nil {
				t.Fatal(err)
			}
			restored, err := current.UnwrapSession(id, tls.ConnectionState{})
			if err != nil || restored == nil || restored.EarlyData != tc.accept {
				t.Fatal(restored, err)
			}
			if !issued.EarlyData || restored == issued {
				t.Fatal("ticket mutation")
			}
			// Client-local cache uses the same exact-map scope alongside origin/policy.
			scope := "origin" + additionalSettingsScope(tc.extra)
			cache := &httpSessionCache{base: tls.NewLRUClientSessionCache(1), scope: scope}
			snapshot := http3conn.SettingsSnapshot{MaxFieldSectionSize: 65536}
			cache.publishSettings(snapshot)
			cache.Put("host", cacheTestTicket(t))
			cache.scope = "origin" + additionalSettingsScope(seed.AdditionalSettings)
			ticket, ok := cache.Get("host")
			if !ok {
				t.Fatal("ordinary resumption lost")
			}
			_, state, err := ticket.ResumptionState()
			if err != nil || state.EarlyData != tc.accept {
				t.Fatal(state, err)
			}
		})
	}
}
