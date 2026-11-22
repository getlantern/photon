package http3wire

import "github.com/getlantern/photon/internal/wire"

const (
	QPACKMaxTableCapacity uint64 = 1
	MaxFieldSectionSize   uint64 = 6
	QPACKBlockedStreams   uint64 = 7
	EnableConnectProtocol uint64 = 8
	H3Datagram            uint64 = 0x33
)

type Setting struct{ ID, Value uint64 }

// ParseSettings parses a complete SETTINGS payload with a caller-defined entry
// cap. Unknown identifiers are retained for duplicate detection and extension
// dispatch; an endpoint must ignore settings it does not understand.
func ParseSettings(payload []byte, maxEntries int) ([]Setting, error) {
	var result []Setting
	seen := make(map[uint64]bool)
	for len(payload) > 0 {
		if len(result) >= maxEntries {
			return nil, &Error{ExcessiveLoad, "settings entry limit"}
		}
		id, n, err := wire.ConsumeVarint(payload)
		if err != nil {
			return nil, &Error{FrameError, "truncated setting identifier"}
		}
		payload = payload[n:]
		value, n, err := wire.ConsumeVarint(payload)
		if err != nil {
			return nil, &Error{FrameError, "truncated setting value"}
		}
		payload = payload[n:]
		if id >= 2 && id <= 5 {
			return nil, &Error{SettingsError, "HTTP/2-only setting"}
		}
		if (id == EnableConnectProtocol || id == H3Datagram) && value > 1 {
			return nil, &Error{SettingsError, "invalid boolean setting value"}
		}
		if seen[id] {
			return nil, &Error{SettingsError, "duplicate setting identifier"}
		}
		seen[id] = true
		result = append(result, Setting{id, value})
	}
	return result, nil
}

// AppendSettings emits a complete frame. Invalid or duplicate identifiers and
// out-of-range values leave the destination unchanged.
func AppendSettings(dst []byte, settings []Setting) ([]byte, error) {
	seen := make(map[uint64]bool)
	var size uint64
	for _, s := range settings {
		if s.ID > wire.MaxVarint || s.Value > wire.MaxVarint {
			return dst, wire.ErrVarintRange
		}
		if (s.ID == EnableConnectProtocol || s.ID == H3Datagram) && s.Value > 1 {
			return dst, &Error{SettingsError, "invalid boolean setting value"}
		}
		if seen[s.ID] || (s.ID >= 2 && s.ID <= 5) {
			return dst, &Error{SettingsError, "duplicate or HTTP/2-only setting"}
		}
		seen[s.ID] = true
		size += uint64(wire.VarintLen(s.ID) + wire.VarintLen(s.Value))
	}
	if size > wire.MaxVarint {
		return dst, wire.ErrVarintRange
	}
	dst, _ = AppendHeader(dst, Header{Settings, size})
	for _, s := range settings {
		dst, _ = wire.AppendVarint(dst, s.ID)
		dst, _ = wire.AppendVarint(dst, s.Value)
	}
	return dst, nil
}
