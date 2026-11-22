package interop

import (
	"crypto/tls"
	"errors"
	own "github.com/getlantern/photon/http3"
	peer "github.com/quic-go/quic-go/http3"
	"testing"
)

func TestHTTP3TLSConfigCompatibility(t *testing.T) {
	if own.NextProtoH3 != peer.NextProtoH3 || own.CapsuleProtocolHeader != peer.CapsuleProtocolHeader {
		t.Fatal("public protocol constants differ")
	}
	for _, implementation := range []struct {
		name      string
		configure func(*tls.Config) *tls.Config
	}{{"own", own.ConfigureTLSConfig}, {"upstream", peer.ConfigureTLSConfig}} {
		t.Run(implementation.name, func(t *testing.T) {
			leaf := &tls.Config{NextProtos: []string{"leaf"}, MinVersion: tls.VersionTLS13}
			selected := &tls.Config{NextProtos: []string{"selected"}, GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) { return leaf, nil }}
			source := &tls.Config{NextProtos: []string{"source"}, GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) { return selected, nil }}
			result := implementation.configure(source)
			inner, err := result.GetConfigForClient(&tls.ClientHelloInfo{})
			if err != nil {
				t.Fatal(err)
			}
			last, err := inner.GetConfigForClient(&tls.ClientHelloInfo{})
			if err != nil {
				t.Fatal(err)
			}
			for _, config := range []*tls.Config{result, inner, last} {
				if len(config.NextProtos) != 1 || config.NextProtos[0] != "h3" {
					t.Fatal("nested ALPN not configured")
				}
			}
			if result == source || inner == selected || last == leaf || last.MinVersion != tls.VersionTLS13 {
				t.Fatal("clone policy changed")
			}
			if source.NextProtos[0] != "source" || selected.NextProtos[0] != "selected" || leaf.NextProtos[0] != "leaf" {
				t.Fatal("application configuration mutated")
			}
			for _, failure := range []error{nil, errors.New("callback failure")} {
				source.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) { return nil, failure }
				result = implementation.configure(source)
				got, err := result.GetConfigForClient(nil)
				if got != nil || err != failure {
					t.Fatal("fallback/error contract changed")
				}
			}
		})
	}
}
