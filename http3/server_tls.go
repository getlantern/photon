package http3

import (
	"bytes"
	"crypto/tls"
	"fmt"

	"github.com/getlantern/photon/internal/http3conn"
	h3 "github.com/getlantern/photon/internal/http3wire"
)

const httpServerTicketPrefix = "independent-quic/http3/server-settings/1\x00"

func (s *Server) settings(headers int) []h3.Setting {
	settings := []h3.Setting{{ID: h3.QPACKMaxTableCapacity, Value: 4096}, {ID: h3.QPACKBlockedStreams, Value: 8}, {ID: h3.MaxFieldSectionSize, Value: uint64(headers)}}
	if len(s.ExtendedConnectHandlers) != 0 || len(s.DatagramHandlers) != 0 {
		settings = append(settings, h3.Setting{ID: h3.EnableConnectProtocol, Value: 1})
	}
	if s.EnableDatagrams || len(s.DatagramHandlers) != 0 {
		settings = append(settings, h3.Setting{ID: h3.H3Datagram, Value: 1})
	}
	return appendAdditionalSettings(settings, s.AdditionalSettings)
}

// ConfigureTLSConfig clones config, selects h3 ALPN and binds server HTTP SETTINGS
// to authenticated TLS tickets. Pass the result to the QUIC listener before
// enabling Allow0RTT. Keep this Server's configuration unchanged while using it.
// Application ticket callbacks and GetConfigForClient are preserved and wrapped;
// selected configurations receive the same HTTP policy. Missing or incompatible
// HTTP ticket metadata disables early data but preserves ordinary resumption.
// This does not implement an application replay policy or enable QUIC early data.
func (s *Server) ConfigureTLSConfig(config *tls.Config) (*tls.Config, error) {
	if config == nil {
		return nil, fmt.Errorf("http3: nil server TLS configuration")
	}
	_, headers, _, err := s.limits()
	if err != nil {
		return nil, err
	}
	settings := s.settings(headers)
	remembered, err := http3conn.RememberSettings(settings)
	if err != nil {
		return nil, err
	}
	scope := "h3" + additionalSettingsScope(s.AdditionalSettings)
	record := encodeHTTPSettingsRecord(httpServerTicketPrefix, scope, remembered.Snapshot())
	return bindHTTPServerTLS(config, settings, record, scope), nil
}

func privateHTTPState(state *tls.SessionState) (*tls.SessionState, error) {
	if state == nil {
		return nil, fmt.Errorf("http3: nil TLS session state")
	}
	encoded, err := state.Bytes()
	if err != nil {
		return nil, err
	}
	return tls.ParseSessionState(encoded)
}

func bindHTTPServerTLS(source *tls.Config, settings []h3.Setting, record []byte, scope string) *tls.Config {
	config := source.Clone()
	config.NextProtos = []string{"h3"}
	wrap := source.WrapSession
	if wrap == nil {
		wrap = source.EncryptTicket
	}
	config.WrapSession = func(cs tls.ConnectionState, state *tls.SessionState) ([]byte, error) {
		private, err := privateHTTPState(state)
		if err != nil {
			return nil, err
		}
		extra := make([][]byte, 0, len(private.Extra)+1)
		for _, b := range private.Extra {
			if !bytes.HasPrefix(b, []byte(httpServerTicketPrefix)) {
				extra = append(extra, b)
			}
		}
		private.Extra = append(extra, bytes.Clone(record))
		return wrap(cs, private)
	}
	unwrap := source.UnwrapSession
	if unwrap == nil {
		unwrap = source.DecryptTicket
	}
	config.UnwrapSession = func(identity []byte, cs tls.ConnectionState) (*tls.SessionState, error) {
		state, err := unwrap(identity, cs)
		if err != nil || state == nil {
			return state, err
		}
		private, err := privateHTTPState(state)
		if err != nil {
			return nil, err
		}
		previous, valid := decodeHTTPSettingsRecord(httpServerTicketPrefix, private.Extra, scope)
		if valid {
			remembered, err := http3conn.RememberSettings(previous)
			valid = err == nil && remembered.ValidateAccepted(settings) == nil
		}
		private.EarlyData = private.EarlyData && valid
		return private, nil
	}
	if selectConfig := source.GetConfigForClient; selectConfig != nil {
		config.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			selected, err := selectConfig(hello)
			if err != nil || selected == nil {
				return selected, err
			}
			return bindHTTPServerTLS(selected, settings, record, scope), nil
		}
	}
	return config
}
