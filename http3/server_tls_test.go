package http3

import (
	"bytes"
	"crypto/tls"
	"errors"
	"net/http"
	"testing"

	h3 "github.com/getlantern/photon/internal/http3wire"
)

func TestHTTPServerTicketSettingsPolicy(t *testing.T) {
	_, original, err := cloneHTTPSession(cacheTestTicket(t))
	if err != nil {
		t.Fatal(err)
	}
	original.EarlyData = true
	original.Extra = [][]byte{[]byte("transport metadata")}
	var issued *tls.SessionState
	source := &tls.Config{NextProtos: []string{"original"}}
	source.WrapSession = func(_ tls.ConnectionState, state *tls.SessionState) ([]byte, error) {
		issued = state
		return []byte("identity"), nil
	}
	source.UnwrapSession = func([]byte, tls.ConnectionState) (*tls.SessionState, error) { return issued, nil }
	seed := &Server{MaxHeaderBytes: 1024, EnableDatagrams: true, ExtendedConnectHandlers: map[string]http.Handler{"test": http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})}}
	bound, err := seed.ConfigureTLSConfig(source)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := bound.WrapSession(tls.ConnectionState{}, original)
	if err != nil || string(identity) != "identity" {
		t.Fatal(string(identity), err)
	}
	if issued == original || len(original.Extra) != 1 || !bytes.Equal(issued.Extra[0], original.Extra[0]) {
		t.Fatal("caller state mutated or opaque metadata lost")
	}
	if source.NextProtos[0] != "original" || bound.NextProtos[0] != "h3" {
		t.Fatal("ALPN ownership")
	}
	for _, tc := range []struct {
		name   string
		server *Server
		accept bool
	}{
		{"same", &Server{MaxHeaderBytes: 1024, EnableDatagrams: true, ExtendedConnectHandlers: seed.ExtendedConnectHandlers}, true},
		{"increased", &Server{MaxHeaderBytes: 2048, EnableDatagrams: true, ExtendedConnectHandlers: seed.ExtendedConnectHandlers}, true},
		{"reduced", &Server{MaxHeaderBytes: 512, EnableDatagrams: true, ExtendedConnectHandlers: seed.ExtendedConnectHandlers}, false},
		{"no datagrams", &Server{MaxHeaderBytes: 1024, ExtendedConnectHandlers: seed.ExtendedConnectHandlers}, false},
		{"no connect", &Server{MaxHeaderBytes: 1024, EnableDatagrams: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current, err := tc.server.ConfigureTLSConfig(source)
			if err != nil {
				t.Fatal(err)
			}
			restored, err := current.UnwrapSession(identity, tls.ConnectionState{})
			if err != nil || restored == nil || restored.EarlyData != tc.accept {
				t.Fatal("restored policy", restored, err)
			}
			if restored == issued || !issued.EarlyData {
				t.Fatal("shared callback state mutated")
			}
			restored.Extra[0][0] ^= 1
			if string(issued.Extra[0]) != "transport metadata" {
				t.Fatal("shared extra mutated")
			}
		})
	}
	saved := issued
	for _, mode := range []string{"missing", "duplicate", "truncated", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			issued, err = privateHTTPState(saved)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "missing":
				issued.Extra = issued.Extra[:1]
			case "duplicate":
				issued.Extra = append(issued.Extra, bytes.Clone(issued.Extra[1]))
			case "truncated":
				issued.Extra[1] = issued.Extra[1][:len(issued.Extra[1])-1]
			case "disabled":
				issued.EarlyData = false
			}
			restored, err := bound.UnwrapSession(identity, tls.ConnectionState{})
			if err != nil || restored == nil || restored.EarlyData {
				t.Fatal("unsafe early restore", restored, err)
			}
		})
	}
	issued = saved
	// Refresh replaces inherited policy while retaining other opaque namespaces.
	if _, err := bound.WrapSession(tls.ConnectionState{}, saved); err != nil {
		t.Fatal(err)
	}
	if len(issued.Extra) != 2 {
		t.Fatal("duplicate refreshed policy")
	}
	settings, valid := decodeHTTPSettingsRecord(httpServerTicketPrefix, issued.Extra, "h3")
	if !valid {
		t.Fatal("missing refreshed record")
	}
	found := false
	for _, setting := range settings {
		if setting.ID == h3.MaxFieldSectionSize && setting.Value == 1024 {
			found = true
		}
	}
	if !found {
		t.Fatal("wrong advertised header limit")
	}
}

func TestHTTPServerSelectedTLSAndCallbacks(t *testing.T) {
	failure := errors.New("application ticket callback")
	selected := &tls.Config{NextProtos: []string{"selected"}, WrapSession: func(tls.ConnectionState, *tls.SessionState) ([]byte, error) { return nil, failure }, UnwrapSession: func([]byte, tls.ConnectionState) (*tls.SessionState, error) { return nil, failure }}
	source := &tls.Config{GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) { return selected, nil }}
	bound, err := (&Server{}).ConfigureTLSConfig(source)
	if err != nil {
		t.Fatal(err)
	}
	got, err := bound.GetConfigForClient(&tls.ClientHelloInfo{})
	if err != nil || got == selected || got.NextProtos[0] != "h3" || selected.NextProtos[0] != "selected" {
		t.Fatal("selected config ownership", err)
	}
	_, state, err := cloneHTTPSession(cacheTestTicket(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := got.WrapSession(tls.ConnectionState{}, state); err != failure {
		t.Fatal("wrap callback", err)
	}
	if _, err := got.UnwrapSession(nil, tls.ConnectionState{}); err != failure {
		t.Fatal("unwrap callback", err)
	}
	source.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) { return nil, failure }
	bound, err = (&Server{}).ConfigureTLSConfig(source)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bound.GetConfigForClient(nil); err != failure {
		t.Fatal("selection callback", err)
	}
	if _, err := (&Server{}).ConfigureTLSConfig(nil); err == nil {
		t.Fatal("nil config accepted")
	}
	if _, err := (&Server{MaxHeaderBytes: -1}).ConfigureTLSConfig(&tls.Config{}); err == nil {
		t.Fatal("invalid server accepted")
	}
}

func TestHTTPServerDefaultTicketEncryption(t *testing.T) {
	_, state, err := cloneHTTPSession(cacheTestTicket(t))
	if err != nil {
		t.Fatal(err)
	}
	state.EarlyData = true
	source := &tls.Config{}
	// The same application TLS config owns encryption keys across wrappers.
	issued, err := (&Server{MaxHeaderBytes: 2048}).ConfigureTLSConfig(source)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := issued.WrapSession(tls.ConnectionState{}, state)
	if err != nil {
		t.Fatal(err)
	}
	for _, headers := range []int{2048, 1024} {
		resumed, err := (&Server{MaxHeaderBytes: headers}).ConfigureTLSConfig(source)
		if err != nil {
			t.Fatal(err)
		}
		restored, err := resumed.UnwrapSession(identity, tls.ConnectionState{})
		if err != nil || restored == nil || restored.EarlyData != (headers == 2048) {
			t.Fatal("default ticket policy", headers, restored, err)
		}
	}
	identity[len(identity)-1] ^= 1
	if restored, err := issued.UnwrapSession(identity, tls.ConnectionState{}); err != nil || restored != nil {
		t.Fatal("unauthenticated ticket accepted", restored, err)
	}
}
