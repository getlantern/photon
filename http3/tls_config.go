package http3

import "crypto/tls"

// NextProtoH3 is the HTTP/3 ALPN identifier for QUIC v1 and v2.
const NextProtoH3 = "h3"

// CapsuleProtocolHeader is the Structured Field identifying capsule support.
const CapsuleProtocolHeader = "Capsule-Protocol"

// ConfigureTLSConfig returns a clone configured to negotiate HTTP/3, including
// configurations selected by GetConfigForClient. The input must be non-nil and
// must not be modified concurrently. Certificate and ticket callbacks are kept.
// Use Server.ConfigureTLSConfig when binding HTTP SETTINGS to 0-RTT tickets.
func ConfigureTLSConfig(source *tls.Config) *tls.Config {
	if source == nil {
		panic("http3: nil TLS configuration")
	}
	// Initialize the source's automatic ticket keys before cloning. Otherwise
	// separately configured listeners could generate unrelated keys on first use.
	_, _ = source.DecryptTicket(nil, tls.ConnectionState{})
	configured := source.Clone()
	configured.NextProtos = []string{NextProtoH3}
	selectConfig := source.GetConfigForClient
	if selectConfig != nil {
		configured.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			selected, err := selectConfig(hello)
			if err == nil && selected != nil {
				selected = ConfigureTLSConfig(selected)
			}
			return selected, err
		}
	}
	return configured
}
