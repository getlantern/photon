package http3

import (
	"github.com/getlantern/photon/internal/http3conn"
	h3 "github.com/getlantern/photon/internal/http3wire"
)

// Settings describes the peer's live HTTP/3 settings.
type Settings struct {
	EnableDatagrams       bool
	EnableExtendedConnect bool
	// Other holds settings besides DATAGRAM, Extended CONNECT and the maximum
	// field-section size. This includes explicitly advertised QPACK settings.
	Other map[uint64]uint64
}

// Settingser exposes settings received from an HTTP/3 peer.
type Settingser interface {
	ReceivedSettings() <-chan struct{}
	Settings() *Settings
}

var _ Settingser = (*ClientConn)(nil)

// ReceivedSettings closes when valid live server SETTINGS arrive. Remembered
// 0-RTT settings do not close it. Also select on the connection or caller context
// when waiting: shutdown before SETTINGS leaves this channel open.
func (c *ClientConn) ReceivedSettings() <-chan struct{} { return c.session.ReceivedSettings() }

// Settings returns a snapshot after ReceivedSettings closes, or nil before it.
// The returned map belongs to the caller; modifying it cannot change negotiation
// or later snapshots. Rejected early sessions retain their own receipt state;
// use the client returned by NextConnection for the replacement session.
func (c *ClientConn) Settings() *Settings { return sessionSettings{c.session}.Settings() }

type sessionSettings struct{ session *http3conn.Session }

func (s sessionSettings) ReceivedSettings() <-chan struct{} { return s.session.ReceivedSettings() }
func (s sessionSettings) Settings() *Settings {
	values, received := s.session.PeerSettings()
	if !received {
		return nil
	}
	result := &Settings{}
	for _, v := range values {
		switch v.ID {
		case h3.H3Datagram:
			result.EnableDatagrams = v.Value == 1
		case h3.EnableConnectProtocol:
			result.EnableExtendedConnect = v.Value == 1
		case h3.MaxFieldSectionSize:
		default:
			if result.Other == nil {
				result.Other = make(map[uint64]uint64)
			}
			result.Other[v.ID] = v.Value
		}
	}
	return result
}

// settingsView preserves the connection binding through body ownership and
// decompression wrappers. A nil source is used only by standalone body fixtures.
type settingsView struct{ sourceSettings Settingser }

func settingsFrom(source any) settingsView {
	provider, _ := source.(Settingser)
	return settingsView{sourceSettings: provider}
}
func (v settingsView) ReceivedSettings() <-chan struct{} {
	if v.sourceSettings == nil {
		return nil
	}
	return v.sourceSettings.ReceivedSettings()
}
func (v settingsView) Settings() *Settings {
	if v.sourceSettings == nil {
		return nil
	}
	return v.sourceSettings.Settings()
}
