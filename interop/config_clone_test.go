package interop

import (
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

// Exercise the same observable shallow-copy contract on each public API.
func TestConfigCloneContract(t *testing.T) {
	for _, independent := range []bool{false, true} {
		name := "upstream"
		if independent {
			name = "independent"
		}
		t.Run(name, func(t *testing.T) {
			if independent {
				input := &quic.Config{Versions: []quic.Version{quic.Version1, quic.Version2}, HandshakeIdleTimeout: time.Second, EnableDatagrams: true}
				cloned := input.Clone()
				if cloned == input || cloned.HandshakeIdleTimeout != time.Second || !cloned.EnableDatagrams {
					t.Fatal("clone did not preserve independent scalar fields")
				}
				cloned.HandshakeIdleTimeout = 2 * time.Second
				cloned.EnableDatagrams = false
				if input.HandshakeIdleTimeout != time.Second || !input.EnableDatagrams {
					t.Fatal("scalar mutation changed original")
				}
				cloned.Versions[0] = quic.Version2
				if input.Versions[0] != quic.Version2 {
					t.Fatal("clone unexpectedly deep-copied versions")
				}
				cloned.Versions = []quic.Version{quic.Version1}
				if len(input.Versions) != 2 || input.Versions[0] != quic.Version2 {
					t.Fatal("slice reassignment changed original")
				}
			} else {
				input := &upstream.Config{Versions: []upstream.Version{upstream.Version1, upstream.Version2}, HandshakeIdleTimeout: time.Second, EnableDatagrams: true}
				cloned := input.Clone()
				if cloned == input || cloned.HandshakeIdleTimeout != time.Second || !cloned.EnableDatagrams {
					t.Fatal("clone did not preserve independent scalar fields")
				}
				cloned.HandshakeIdleTimeout = 2 * time.Second
				cloned.EnableDatagrams = false
				if input.HandshakeIdleTimeout != time.Second || !input.EnableDatagrams {
					t.Fatal("scalar mutation changed original")
				}
				cloned.Versions[0] = upstream.Version2
				if input.Versions[0] != upstream.Version2 {
					t.Fatal("clone unexpectedly deep-copied versions")
				}
				cloned.Versions = []upstream.Version{upstream.Version1}
				if len(input.Versions) != 2 || input.Versions[0] != upstream.Version2 {
					t.Fatal("slice reassignment changed original")
				}
			}
		})
	}
}
