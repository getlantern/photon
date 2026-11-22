package wire

import "errors"

var ErrEarlyTransport = errors.New("quic: invalid remembered transport state")

// EarlyTransport retains the reusable RFC 9000 parameters plus DATAGRAM credit
// and reliable stream reset support.
// Extension-specific version negotiation and QUIC-bit token state are separate.
// Connection IDs, ACK policy and multipath negotiation are never copied here.
type EarlyTransport struct {
	EarlyLimits
	MaxIdleTimeout         uint64
	MaxUDPPayloadSize      uint64
	DisableActiveMigration bool
	ResetStreamAt          bool
}

func (p TransportParameters) EarlyTransport() EarlyTransport {
	return EarlyTransport{p.EarlyLimits(), p.MaxIdleTimeout, p.MaxUDPPayloadSize, p.DisableActiveMigration, p.ResetStreamAt}
}

func (s EarlyTransport) valid() bool {
	return s.EarlyLimits.valid() && s.MaxIdleTimeout <= MaxVarint && s.MaxUDPPayloadSize >= 1200 && s.MaxUDPPayloadSize <= MaxVarint
}

// Supports checks remembered credit and extension promises required for 0-RTT.
// It does not establish replay safety or other application acceptance policies.
func (s EarlyTransport) Supports(previous EarlyTransport) bool {
	return s.valid() && previous.valid() && s.EarlyLimits.Supports(previous.EarlyLimits) &&
		(!previous.ResetStreamAt || s.ResetStreamAt)
}

// Marshal encodes schema 3, including the bounded schema-1 credit record.
// Schema 2 remains readable and implies no reliable stream reset support.
// Older credit-only records are insufficient to restore this state.
func (s EarlyTransport) Marshal() ([]byte, error) {
	if !s.valid() {
		return nil, ErrEarlyTransport
	}
	limits, err := s.EarlyLimits.Marshal()
	if err != nil {
		return nil, err
	}
	b := append([]byte{3, byte(len(limits))}, limits...)
	b, _ = AppendVarint(b, s.MaxIdleTimeout)
	b, _ = AppendVarint(b, s.MaxUDPPayloadSize)
	flag := byte(0)
	if s.DisableActiveMigration {
		flag = 1
	}
	b = append(b, flag)
	flag = 0
	if s.ResetStreamAt {
		flag = 1
	}
	return append(b, flag), nil
}

func ParseEarlyTransport(b []byte) (EarlyTransport, error) {
	var s EarlyTransport
	if len(b) < 15 || len(b) > 85 || (b[0] != 2 && b[0] != 3) {
		return s, ErrEarlyTransport
	}
	schema := b[0]
	n := int(b[1])
	if n < 9 || n > 65 || n > len(b)-2 {
		return s, ErrEarlyTransport
	}
	limits, err := ParseEarlyLimits(b[2 : 2+n])
	if err != nil {
		return s, ErrEarlyTransport
	}
	s.EarlyLimits = limits
	b = b[2+n:]
	for _, out := range []*uint64{&s.MaxIdleTimeout, &s.MaxUDPPayloadSize} {
		v, n, err := ConsumeVarint(b)
		if err != nil {
			return EarlyTransport{}, ErrEarlyTransport
		}
		*out = v
		b = b[n:]
	}
	flags := 1
	if schema == 3 {
		flags = 2
	}
	if len(b) != flags || b[0] > 1 {
		return EarlyTransport{}, ErrEarlyTransport
	}
	s.DisableActiveMigration = b[0] == 1
	if schema == 3 {
		if b[1] > 1 {
			return EarlyTransport{}, ErrEarlyTransport
		}
		s.ResetStreamAt = b[1] == 1
	}
	if !s.valid() {
		return EarlyTransport{}, ErrEarlyTransport
	}
	return s, nil
}
