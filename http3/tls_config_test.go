package http3

import (
	"bytes"
	"crypto/tls"
	"errors"
	"testing"
)

func TestConfigureTLSConfigSelection(t *testing.T) {
	failure := errors.New("selection failed")
	for _, mode := range []string{"selected", "fallback", "error", "selected-error"} {
		t.Run(mode, func(t *testing.T) {
			selected := &tls.Config{NextProtos: []string{"selected-original"}, MinVersion: tls.VersionTLS13}
			hello := &tls.ClientHelloInfo{ServerName: "example.test"}
			var calls int
			source := &tls.Config{NextProtos: []string{"original"}, GetConfigForClient: func(got *tls.ClientHelloInfo) (*tls.Config, error) {
				calls++
				if got != hello {
					t.Fatal("ClientHello identity changed")
				}
				switch mode {
				case "fallback":
					return nil, nil
				case "error":
					return nil, failure
				case "selected-error":
					return selected, failure
				}
				return selected, nil
			}}
			configured := ConfigureTLSConfig(source)
			if configured == source || len(configured.NextProtos) != 1 || configured.NextProtos[0] != NextProtoH3 {
				t.Fatal("ALPN clone missing")
			}
			got, err := configured.GetConfigForClient(hello)
			if calls != 1 {
				t.Fatal("callback invocation count")
			}
			switch mode {
			case "selected":
				if err != nil || got == selected || got.NextProtos[0] != NextProtoH3 || got.MinVersion != tls.VersionTLS13 {
					t.Fatal("selected configuration not cloned")
				}
				got.NextProtos[0] = "changed"
			case "fallback":
				if got != nil || err != nil {
					t.Fatal("fallback changed")
				}
			case "error":
				if got != nil || err != failure {
					t.Fatal("selection error changed")
				}
			case "selected-error":
				if got != selected || err != failure {
					t.Fatal("error result was rewritten")
				}
			}
			configured.NextProtos[0] = "changed"
			if source.NextProtos[0] != "original" || selected.NextProtos[0] != "selected-original" {
				t.Fatal("caller ALPN mutated")
			}
		})
	}
}

func TestConfigureTLSConfigTickets(t *testing.T) {
	_, state, err := cloneHTTPSession(cacheTestTicket(t))
	if err != nil {
		t.Fatal(err)
	}
	state.Extra = [][]byte{[]byte("application metadata")}
	source := &tls.Config{}
	first := ConfigureTLSConfig(source)
	second := ConfigureTLSConfig(source)
	ticket, err := first.EncryptTicket(tls.ConnectionState{}, state)
	if err != nil {
		t.Fatal(err)
	}
	for _, config := range []*tls.Config{source, second} {
		restored, err := config.DecryptTicket(ticket, tls.ConnectionState{})
		if err != nil || restored == nil || len(restored.Extra) != 1 || !bytes.Equal(restored.Extra[0], state.Extra[0]) {
			t.Fatal("clones do not share source ticket keys", err)
		}
	}
	var wrap, unwrap, certificate bool
	source.WrapSession = func(tls.ConnectionState, *tls.SessionState) ([]byte, error) {
		wrap = true
		return []byte("custom"), nil
	}
	source.UnwrapSession = func([]byte, tls.ConnectionState) (*tls.SessionState, error) { unwrap = true; return state, nil }
	source.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) { certificate = true; return nil, nil }
	configured := ConfigureTLSConfig(source)
	configured.WrapSession(tls.ConnectionState{}, state)
	configured.UnwrapSession(nil, tls.ConnectionState{})
	configured.GetCertificate(nil)
	if !wrap || !unwrap || !certificate {
		t.Fatal("application callbacks lost")
	}
}
