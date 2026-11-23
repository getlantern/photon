package http3conn

import (
	h3 "github.com/getlantern/photon/internal/http3wire"
	"sync"
)

// SettingsSnapshot contains understood settings. Absent field-section limits
// mean unlimited, represented by MaxUint64. QPACK defaults to zero capacity and
// zero blocked streams. Unknown settings are ignored, not retained indefinitely.
type SettingsSnapshot struct {
	QPACKCapacity, QPACKBlockedStreams, MaxFieldSectionSize uint64
	ExtendedConnect                                         bool
	HTTPDatagrams                                           bool
}

func settingsSnapshot(settings []h3.Setting) (SettingsSnapshot, error) {
	// Reuse the wire encoder's transactional validation, including duplicates,
	// reserved identifiers, value bounds and extension-specific constraints.
	if _, err := h3.AppendSettings(nil, settings); err != nil {
		return SettingsSnapshot{}, err
	}
	s := SettingsSnapshot{MaxFieldSectionSize: ^uint64(0)}
	for _, v := range settings {
		switch v.ID {
		case h3.QPACKMaxTableCapacity:
			s.QPACKCapacity = v.Value
		case h3.QPACKBlockedStreams:
			s.QPACKBlockedStreams = v.Value
		case h3.MaxFieldSectionSize:
			s.MaxFieldSectionSize = v.Value
		case h3.H3Datagram:
			s.HTTPDatagrams = v.Value == 1
		case h3.EnableConnectProtocol:
			s.ExtendedConnect = v.Value == 1
		}
	}
	return s, nil
}

// SettingsState publishes connection settings safely to request workers. Create
// it from the exact local settings successfully written by OpenLocal, before
// dispatching incoming streams. ApplyPeer consumes the sole peer SETTINGS event.
// NewEarlySettingsState additionally validates remembered server settings.
// The owner must still configure QPACK/field limits from these snapshots and
// close the connection on ApplyPeer failure. Do not copy after first use.
type SettingsState struct {
	remembered  *RememberedSettings
	mu          sync.RWMutex
	server      bool
	local, peer SettingsSnapshot
	received    bool
	err         error
}

func NewSettingsState(server bool, sentLocal []h3.Setting) (*SettingsState, error) {
	local, err := settingsSnapshot(sentLocal)
	if err != nil {
		return nil, err
	}
	return &SettingsState{server: server, local: local, peer: SettingsSnapshot{MaxFieldSectionSize: ^uint64(0)}}, nil
}
func (s *SettingsState) ApplyPeer(settings []h3.Setting) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if s.received {
		s.err = &h3.Error{Code: h3.FrameUnexpected, Reason: "repeated SETTINGS"}
		return s.err
	}
	peer, err := settingsSnapshot(settings)
	if err != nil {
		s.err = err
		return err
	}
	if s.remembered != nil {
		if err := s.remembered.validate(peer, settings); err != nil {
			s.err = err
			return err
		}
	}
	s.peer, s.received = peer, true
	return nil
}
func (s *SettingsState) Peer() (SettingsSnapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.peer, s.received
}
func (s *SettingsState) Local() SettingsSnapshot { return s.local }

// CanSendExtendedConnect requires a server advertisement received by a client.
// A client advertisement has no effect on a server's permission (RFC 9220).
func (s *SettingsState) CanSendExtendedConnect() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.err == nil && !s.server && s.received && s.peer.ExtendedConnect
}

// CanReceiveExtendedConnect depends on our own server advertisement, never the
// client's settings. The owner uses this in MessageConfig.ExtendedConnect.
func (s *SettingsState) CanReceiveExtendedConnect() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.err == nil && s.server && s.local.ExtendedConnect
}

// CanSendHTTPDatagrams is the 1-RTT HTTP negotiation gate (RFC 9297 section
// 2.1.1), symmetric for clients and servers. Construction requires settings
// already written by OpenLocal. Callers must additionally enforce QUIC DATAGRAM
// support, connection/request lifetime, and extension-specific request semantics.
func (s *SettingsState) CanSendHTTPDatagrams() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.err == nil && s.received && s.local.HTTPDatagrams && s.peer.HTTPDatagrams
}
