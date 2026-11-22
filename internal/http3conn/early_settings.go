package http3conn

import h3 "github.com/getlantern/photon/internal/http3wire"

// RememberedSettings contains only understood settings from the connection that
// issued a resumption ticket. It owns no slices or QPACK dynamic table state.
// The owner must bind this value to that ticket, origin and HTTP configuration.
type RememberedSettings struct {
	snapshot SettingsSnapshot
}

func RememberSettings(settings []h3.Setting) (RememberedSettings, error) {
	snapshot, err := settingsSnapshot(settings)
	return RememberedSettings{snapshot: snapshot}, err
}
func (r RememberedSettings) Snapshot() SettingsSnapshot { return r.snapshot }

// ValidateAccepted checks newly received server SETTINGS after accepted 0-RTT.
// Do not apply this to rejected early data: discard its HTTP/QPACK session and
// construct a fresh SettingsState. Unknown settings remain ignored.
func (r RememberedSettings) ValidateAccepted(settings []h3.Setting) error {
	current, err := settingsSnapshot(settings)
	if err != nil {
		return err
	}
	return r.validate(current, settings)
}
func (r RememberedSettings) validate(current SettingsSnapshot, settings []h3.Setting) error {
	old := r.snapshot
	// RFC 9204 section 3.2.3: nonzero remembered capacity must be identical,
	// including explicit presence, unlike the general HTTP limit rule.
	if old.QPACKCapacity != 0 && current.QPACKCapacity != old.QPACKCapacity {
		return &h3.Error{Code: QPACKDecoderStreamError, Reason: "changed remembered QPACK capacity"}
	}
	if current.QPACKBlockedStreams < old.QPACKBlockedStreams || current.MaxFieldSectionSize < old.MaxFieldSectionSize || (old.ExtendedConnect && !current.ExtendedConnect) || (old.HTTPDatagrams && !current.HTTPDatagrams) {
		return &h3.Error{Code: h3.SettingsError, Reason: "incompatible remembered SETTINGS"}
	}
	// RFC 9114 section 7.2.8.2 also forbids omission of an understood
	// previously nondefault setting, even if its default would increase a limit.
	var capacity, blocked, fields, connect, datagrams bool
	for _, setting := range settings {
		switch setting.ID {
		case h3.QPACKMaxTableCapacity:
			capacity = true
		case h3.QPACKBlockedStreams:
			blocked = true
		case h3.MaxFieldSectionSize:
			fields = true
		case h3.EnableConnectProtocol:
			connect = true
		case h3.H3Datagram:
			datagrams = true
		}
	}
	if old.QPACKCapacity != 0 && !capacity {
		return &h3.Error{Code: QPACKDecoderStreamError, Reason: "omitted remembered QPACK capacity"}
	}
	if (old.QPACKBlockedStreams != 0 && !blocked) || (old.MaxFieldSectionSize != ^uint64(0) && !fields) || (old.ExtendedConnect && !connect) || (old.HTTPDatagrams && !datagrams) {
		return &h3.Error{Code: h3.SettingsError, Reason: "omitted remembered setting"}
	}
	return nil
}

// NewEarlySettingsState validates and snapshots remembered server settings.
// It is client-only. ApplyPeer requires compatible SETTINGS because this state
// belongs to accepted early data. On TLS rejection, discard this state together
// with all early HTTP streams and QPACK state; use NewSettingsState for recovery.
// This does not initialize the session encoder or authorize request replay.
func NewEarlySettingsState(sentLocal, remembered []h3.Setting) (*SettingsState, error) {
	r, err := RememberSettings(remembered)
	if err != nil {
		return nil, err
	}
	s, err := NewSettingsState(false, sentLocal)
	if err != nil {
		return nil, err
	}
	s.remembered = &r
	return s, nil
}

// EarlyPeer supplies remembered limits without pretending peer SETTINGS arrived.
// It stops exposing them after live SETTINGS or a terminal validation failure.
func (s *SettingsState) EarlyPeer() (SettingsSnapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.err != nil || s.received || s.remembered == nil {
		return SettingsSnapshot{}, false
	}
	return s.remembered.snapshot, true
}
