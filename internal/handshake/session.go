package handshake

import (
	"crypto/tls"
	"github.com/getlantern/photon/internal/wire"
)

// Session events may change EarlyData. Cache entries can be shared by concurrent
// connections (or another TLS consumer), so resume using a private decoded copy.
// Serialization also preserves opaque session Extra values owned by other users.
type resumeCache struct {
	tls.ClientSessionCache
	version    wire.Version
	allowEarly bool
}

func (c resumeCache) Get(key string) (*tls.ClientSessionState, bool) {
	cached, ok := c.ClientSessionCache.Get(key)
	if !ok || cached == nil {
		return nil, false
	}
	ticket, state, err := cached.ResumptionState()
	if err != nil || state == nil || !ticketVersionMatches(state.Extra, c.version) {
		return nil, false
	}
	encoded, err := state.Bytes()
	if err != nil {
		return nil, false
	}
	private, err := tls.ParseSessionState(encoded)
	if err != nil {
		return nil, false
	}
	_, valid := restoredTicketTransport(private.Extra, c.version)
	private.EarlyData = private.EarlyData && c.allowEarly && valid
	result, err := tls.NewResumptionState(append([]byte(nil), ticket...), private)
	return result, err == nil
}

// Bind both the base and dynamically selected configurations before TLS selects
// a PSK. A QUICResumeSession event is too late to reject ordinary resumption.
func bindServerTickets(dst, source *tls.Config, version wire.Version) {
	dst.ClientSessionCache = nil
	if dst.WrapSession == nil {
		dst.WrapSession = source.EncryptTicket
	}
	unwrap := source.UnwrapSession
	if unwrap == nil {
		unwrap = source.DecryptTicket
	}
	dst.UnwrapSession = func(identity []byte, cs tls.ConnectionState) (*tls.SessionState, error) {
		state, err := unwrap(identity, cs)
		if err != nil || state == nil {
			return state, err
		}
		if !ticketVersionMatches(state.Extra, version) {
			return nil, nil
		}
		return state, nil
	}
	if selectConfig := source.GetConfigForClient; selectConfig != nil {
		dst.GetConfigForClient = func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
			selected, err := selectConfig(hello)
			if err != nil || selected == nil {
				return selected, err
			}
			private := selected.Clone()
			bindServerTickets(private, selected, version)
			return private, nil
		}
	}
}
