package wire

import "errors"

var ErrEarlyLimits = errors.New("quic: invalid remembered early-data limits")

// EarlyLimits is the minimum transport-limit compatibility state required by
// RFC 9000 section 7.4.1 and RFC 9221 section 3. It contains no connection IDs,
// reset tokens, preferred addresses, ACK policy, or multipath negotiation.
// This is not the entire remembered connection state: version, application
// protocol/settings, optional policies and replay decisions need separate gates.
type EarlyLimits struct {
	ActiveConnectionIDLimit        uint64
	InitialMaxData                 uint64
	InitialMaxStreamDataBidiLocal  uint64
	InitialMaxStreamDataBidiRemote uint64
	InitialMaxStreamDataUni        uint64
	InitialMaxStreamsBidi          uint64
	InitialMaxStreamsUni           uint64
	MaxDatagramFrameSize           uint64
}

// EarlyLimits snapshots only scalar limits from validated server parameters.
func (p TransportParameters) EarlyLimits() EarlyLimits {
	return EarlyLimits{p.ActiveConnectionIDLimit, p.InitialMaxData, p.InitialMaxStreamDataBidiLocal, p.InitialMaxStreamDataBidiRemote, p.InitialMaxStreamDataUni, p.InitialMaxStreamsBidi, p.InitialMaxStreamsUni, p.MaxDatagramFrameSize}
}
func (s EarlyLimits) values() [8]uint64 {
	return [8]uint64{s.ActiveConnectionIDLimit, s.InitialMaxData, s.InitialMaxStreamDataBidiLocal, s.InitialMaxStreamDataBidiRemote, s.InitialMaxStreamDataUni, s.InitialMaxStreamsBidi, s.InitialMaxStreamsUni, s.MaxDatagramFrameSize}
}
func (s EarlyLimits) valid() bool {
	if s.ActiveConnectionIDLimit < 2 || s.InitialMaxStreamsBidi > 1<<60 || s.InitialMaxStreamsUni > 1<<60 {
		return false
	}
	for _, v := range s.values() {
		if v > MaxVarint {
			return false
		}
	}
	return true
}

// Supports reports whether current limits support all previously granted credit.
// False requires rejecting early data; true does not establish other conditions
// for acceptance. Both snapshots must be valid, even when compared to themselves.
func (s EarlyLimits) Supports(previous EarlyLimits) bool {
	if !s.valid() || !previous.valid() {
		return false
	}
	now, old := s.values(), previous.values()
	for i := range now {
		if now[i] < old[i] {
			return false
		}
	}
	return true
}

// Marshal encodes private ticket metadata, not QUIC transport parameters.
// Schema 1 contains exactly eight varints and is bounded at 65 bytes. Callers
// must authenticate this metadata with the enclosing TLS session ticket.
func (s EarlyLimits) Marshal() ([]byte, error) {
	if !s.valid() {
		return nil, ErrEarlyLimits
	}
	b := make([]byte, 1, 65)
	b[0] = 1
	for _, v := range s.values() {
		b, _ = AppendVarint(b, v)
	}
	return b, nil
}
func ParseEarlyLimits(b []byte) (EarlyLimits, error) {
	if len(b) < 9 || len(b) > 65 || b[0] != 1 {
		return EarlyLimits{}, ErrEarlyLimits
	}
	b = b[1:]
	var values [8]uint64
	for i := range values {
		v, n, err := ConsumeVarint(b)
		if err != nil {
			return EarlyLimits{}, ErrEarlyLimits
		}
		values[i] = v
		b = b[n:]
	}
	s := EarlyLimits{values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7]}
	if len(b) != 0 || !s.valid() {
		return EarlyLimits{}, ErrEarlyLimits
	}
	return s, nil
}
