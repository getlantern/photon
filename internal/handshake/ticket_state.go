package handshake

import (
	"bytes"
	"encoding/binary"

	"github.com/getlantern/photon/internal/wire"
)

// Extra is shared with application ticket callbacks. A namespaced record keeps
// unrelated entries intact. The enclosed limits codec has its own schema byte.
const ticketStatePrefix = "getlantern/photon/early-limits\x00"

func ticketState(version wire.Version, limits wire.EarlyTransport) ([]byte, error) {
	if !version.Supported() {
		return nil, wire.ErrVersion
	}
	encoded, err := limits.Marshal()
	if err != nil {
		return nil, err
	}
	out := binary.BigEndian.AppendUint32([]byte(ticketStatePrefix), uint32(version))
	return append(out, encoded...), nil
}

// Restored limits are only evidence for one acceptance check. Missing, duplicate,
// malformed or cross-version metadata cannot authorize early data. The separate
// ticket version gate rejects unproven origins before ordinary TLS resumption;
// malformed transport state with a proven origin can still resume without 0-RTT.
func restoredTicketTransport(extra [][]byte, version wire.Version) (wire.EarlyTransport, bool) {
	var result wire.EarlyTransport
	found := false
	for _, record := range extra {
		if !bytes.HasPrefix(record, []byte(ticketStatePrefix)) {
			continue
		}
		if found {
			return wire.EarlyTransport{}, false
		}
		found = true
		payload := record[len(ticketStatePrefix):]
		if len(payload) < 4 || !version.Supported() || wire.Version(binary.BigEndian.Uint32(payload)) != version {
			return wire.EarlyTransport{}, false
		}
		var err error
		result, err = wire.ParseEarlyTransport(payload[4:])
		if err != nil {
			return wire.EarlyTransport{}, false
		}
	}
	return result, found
}

// ResumedTransport reports version-matched server limits from a ticket (server)
// or its client-local cached state (client). This does
// not indicate that early data was accepted or that other policies are satisfied.
func (h *TLS) ResumedTransport() (wire.EarlyTransport, bool) {
	return h.resumedTransport, h.haveResumedTransport
}

// ticketVersionMatches is independent of the transport schema: a legacy record
// can still identify a ticket's version for ordinary 1-RTT resumption. An absent,
// ambiguous or invalid version cannot establish its origin (RFC 9369 section 5).
func ticketVersionMatches(extra [][]byte, version wire.Version) bool {
	if !version.Supported() {
		return false
	}
	found := false
	for _, record := range extra {
		if !bytes.HasPrefix(record, []byte(ticketStatePrefix)) {
			continue
		}
		if found {
			return false
		}
		found = true
		payload := record[len(ticketStatePrefix):]
		if len(payload) < 4 || wire.Version(binary.BigEndian.Uint32(payload)) != version {
			return false
		}
	}
	return found
}
