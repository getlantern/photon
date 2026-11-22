package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

var ErrTransportParameter = errors.New("quic: transport parameter error")

// ParameterError maps to TRANSPORT_PARAMETER_ERROR (0x08) at the transport layer.
type ParameterError struct {
	ID     uint64
	Reason string
}

func (e *ParameterError) Error() string {
	return fmt.Sprintf("%v: 0x%x: %s", ErrTransportParameter, e.ID, e.Reason)
}
func (e *ParameterError) Unwrap() error             { return ErrTransportParameter }
func parameterError(id uint64, reason string) error { return &ParameterError{ID: id, Reason: reason} }

// PreferredAddress has the layout specified in RFC 9000 section 18.2. An all-zero
// address and port indicates an unused family. Addresses remain wire byte arrays
// so parsing does not silently normalize IPv4-mapped IPv6 addresses.
type PreferredAddress struct {
	IPv4                [4]byte
	IPv4Port            uint16
	IPv6                [16]byte
	IPv6Port            uint16
	ConnectionID        []byte
	StatelessResetToken [16]byte
}

// TransportParameters contains RFC 9000 parameters plus DATAGRAM and QUIC bit
// greasing. Numeric durations use their wire unit (milliseconds), avoiding
// overflow when untrusted values are later converted to time.Duration.
// Nil CID slices mean absent; non-nil empty slices mean a present zero-length CID.
// Construct with DefaultTransportParameters, not a zero value.
type TransportParameters struct {
	// ResetStreamAt advertises reception of draft-ietf-quic-reliable-stream-reset-11
	// frames via the empty reset_stream_at parameter (0x1d).
	ResetStreamAt                   bool
	OriginalDestinationConnectionID []byte
	MaxIdleTimeout                  uint64
	StatelessResetToken             *[16]byte
	MaxUDPPayloadSize               uint64
	InitialMaxData                  uint64
	InitialMaxStreamDataBidiLocal   uint64
	InitialMaxStreamDataBidiRemote  uint64
	InitialMaxStreamDataUni         uint64
	InitialMaxStreamsBidi           uint64
	InitialMaxStreamsUni            uint64
	AckDelayExponent                uint64
	MaxAckDelay                     uint64
	DisableActiveMigration          bool
	PreferredAddress                *PreferredAddress
	ActiveConnectionIDLimit         uint64
	InitialSourceConnectionID       []byte
	RetrySourceConnectionID         []byte
	MaxDatagramFrameSize            uint64
	GreaseQUICBit                   bool
	VersionInformation              *VersionInformation
	// InitialMaxPathID is draft-ietf-quic-multipath-21 parameter 0x3e.
	// Nil means absent; a present zero still advertises multipath support.
	// It must not be remembered for a subsequent connection.
	InitialMaxPathID *uint64
}

func DefaultTransportParameters() TransportParameters {
	return TransportParameters{MaxUDPPayloadSize: 65527, AckDelayExponent: 3, MaxAckDelay: 25, ActiveConnectionIDLimit: 2}
}

// numericParameters is shared by validation and serialization; defaults are
// wire defaults, not endpoint policy. Values greater than the physical UDP
// maximum are permitted by RFC 9000; the sender still obeys path/UDP limits.
func (p *TransportParameters) numericParameters() []struct{ id, value, def uint64 } {
	return []struct{ id, value, def uint64 }{
		{1, p.MaxIdleTimeout, 0}, {3, p.MaxUDPPayloadSize, 65527}, {4, p.InitialMaxData, 0},
		{5, p.InitialMaxStreamDataBidiLocal, 0}, {6, p.InitialMaxStreamDataBidiRemote, 0},
		{7, p.InitialMaxStreamDataUni, 0}, {8, p.InitialMaxStreamsBidi, 0}, {9, p.InitialMaxStreamsUni, 0},
		{10, p.AckDelayExponent, 3}, {11, p.MaxAckDelay, 25}, {14, p.ActiveConnectionIDLimit, 2},
		{0x20, p.MaxDatagramFrameSize, 0},
	}
}

// Validate checks syntax, ranges, mandatory parameters, and sender role. The
// caller must also call ValidateConnectionIDs with observed packet IDs.
func (p *TransportParameters) Validate(senderIsServer bool) error {
	if p.InitialMaxPathID != nil && *p.InitialMaxPathID > 1<<32-1 {
		return parameterError(0x3e, "initial_max_path_id exceeds 2^32-1")
	}
	if p.VersionInformation != nil {
		if err := p.VersionInformation.validate(senderIsServer); err != nil {
			return err
		}
	}
	for _, n := range p.numericParameters() {
		if n.value > MaxVarint {
			return parameterError(n.id, "integer exceeds 62 bits")
		}
	}
	for _, c := range []struct {
		id uint64
		b  []byte
	}{{0, p.OriginalDestinationConnectionID}, {15, p.InitialSourceConnectionID}, {16, p.RetrySourceConnectionID}} {
		if len(c.b) > 20 {
			return parameterError(c.id, "connection ID exceeds 20 bytes")
		}
	}
	if p.InitialSourceConnectionID == nil {
		return parameterError(15, "missing initial source connection ID")
	}
	if senderIsServer {
		if p.OriginalDestinationConnectionID == nil {
			return parameterError(0, "missing original destination connection ID")
		}
	} else {
		for _, s := range []struct {
			id      uint64
			present bool
		}{{0, p.OriginalDestinationConnectionID != nil}, {2, p.StatelessResetToken != nil}, {13, p.PreferredAddress != nil}, {16, p.RetrySourceConnectionID != nil}} {
			if s.present {
				return parameterError(s.id, "server-only parameter sent by client")
			}
		}
	}
	if p.MaxUDPPayloadSize < 1200 {
		return parameterError(3, "UDP payload limit below 1200")
	}
	if p.InitialMaxStreamsBidi > 1<<60 {
		return parameterError(8, "stream limit exceeds 2^60")
	}
	if p.InitialMaxStreamsUni > 1<<60 {
		return parameterError(9, "stream limit exceeds 2^60")
	}
	if p.AckDelayExponent > 20 {
		return parameterError(10, "ACK delay exponent exceeds 20")
	}
	if p.MaxAckDelay >= 1<<14 {
		return parameterError(11, "maximum ACK delay must be below 2^14")
	}
	if p.ActiveConnectionIDLimit < 2 {
		return parameterError(14, "active connection ID limit below 2")
	}
	if a := p.PreferredAddress; a != nil {
		if len(p.InitialSourceConnectionID) == 0 {
			return parameterError(13, "preferred address with zero-length initial connection ID")
		}
		if len(a.ConnectionID) == 0 || len(a.ConnectionID) > 20 {
			return parameterError(13, "preferred connection ID length must be 1..20")
		}
	}
	return nil
}

// ConnectionIDContext contains packet observations, never values copied from
// the peer's transport parameters. RetryUsed distinguishes absence from an
// observed zero-length Retry CID.
type ConnectionIDContext struct {
	PeerIsServer                    bool
	InitialSourceConnectionID       []byte
	OriginalDestinationConnectionID []byte
	RetryUsed                       bool
	RetrySourceConnectionID         []byte
}

func (p *TransportParameters) ValidateConnectionIDs(c ConnectionIDContext) error {
	if err := p.Validate(c.PeerIsServer); err != nil {
		return err
	}
	if !bytes.Equal(p.InitialSourceConnectionID, c.InitialSourceConnectionID) {
		return parameterError(15, "initial source connection ID mismatch")
	}
	if c.PeerIsServer {
		if !bytes.Equal(p.OriginalDestinationConnectionID, c.OriginalDestinationConnectionID) {
			return parameterError(0, "original destination connection ID mismatch")
		}
		if c.RetryUsed != (p.RetrySourceConnectionID != nil) {
			return parameterError(16, "Retry source connection ID presence mismatch")
		}
		if c.RetryUsed && !bytes.Equal(p.RetrySourceConnectionID, c.RetrySourceConnectionID) {
			return parameterError(16, "Retry source connection ID mismatch")
		}
	}
	return nil
}

func copyParameter(b []byte) []byte { out := make([]byte, len(b)); copy(out, b); return out }

// ParseTransportParameters returns an owning copy. Unknown parameters are
// ignored, but duplicate IDs (including unknown IDs and alternate varint
// encodings of an ID) are rejected. The TLS extension is at most 65535 bytes.
func ParseTransportParameters(b []byte, senderIsServer bool) (TransportParameters, error) {
	p := DefaultTransportParameters()
	fail := func(id uint64, reason string) (TransportParameters, error) {
		return TransportParameters{}, parameterError(id, reason)
	}
	if len(b) > 65535 {
		return fail(0, "TLS extension exceeds 65535 bytes")
	}
	seen := make(map[uint64]struct{})
	for len(b) > 0 {
		id, n, err := ConsumeVarint(b)
		if err != nil {
			return fail(0, "truncated parameter ID")
		}
		b = b[n:]
		if _, ok := seen[id]; ok {
			return fail(id, "duplicate parameter")
		}
		seen[id] = struct{}{}
		length, n, err := ConsumeVarint(b)
		if err != nil {
			return fail(id, "truncated parameter length")
		}
		b = b[n:]
		if length > uint64(len(b)) {
			return fail(id, "truncated parameter value")
		}
		value := b[:int(length)]
		b = b[int(length):]
		var target *uint64
		switch id {
		case 0:
			p.OriginalDestinationConnectionID = copyParameter(value)
		case 1:
			target = &p.MaxIdleTimeout
		case 2:
			if len(value) != 16 {
				return fail(id, "stateless reset token must be 16 bytes")
			}
			p.StatelessResetToken = new([16]byte)
			copy(p.StatelessResetToken[:], value)
		case 3:
			target = &p.MaxUDPPayloadSize
		case 4:
			target = &p.InitialMaxData
		case 5:
			target = &p.InitialMaxStreamDataBidiLocal
		case 6:
			target = &p.InitialMaxStreamDataBidiRemote
		case 7:
			target = &p.InitialMaxStreamDataUni
		case 8:
			target = &p.InitialMaxStreamsBidi
		case 9:
			target = &p.InitialMaxStreamsUni
		case 10:
			target = &p.AckDelayExponent
		case 11:
			target = &p.MaxAckDelay
		case 12:
			if len(value) != 0 {
				return fail(id, "disable_active_migration must be empty")
			}
			p.DisableActiveMigration = true
		case 13:
			if len(value) < 41 {
				return fail(id, "truncated preferred address")
			}
			cidLen := int(value[24])
			if cidLen < 1 || cidLen > 20 || len(value) != 41+cidLen {
				return fail(id, "invalid preferred address length")
			}
			a := new(PreferredAddress)
			copy(a.IPv4[:], value[:4])
			a.IPv4Port = binary.BigEndian.Uint16(value[4:6])
			copy(a.IPv6[:], value[6:22])
			a.IPv6Port = binary.BigEndian.Uint16(value[22:24])
			a.ConnectionID = copyParameter(value[25 : 25+cidLen])
			copy(a.StatelessResetToken[:], value[25+cidLen:])
			p.PreferredAddress = a
		case 14:
			target = &p.ActiveConnectionIDLimit
		case 15:
			p.InitialSourceConnectionID = copyParameter(value)
		case 16:
			p.RetrySourceConnectionID = copyParameter(value)
		case 0x11:
			var err error
			p.VersionInformation, err = parseVersionInformation(value, senderIsServer)
			if err != nil {
				return TransportParameters{}, err
			}
		case 0x1d:
			if len(value) != 0 {
				return fail(id, "reset_stream_at must be empty")
			}
			p.ResetStreamAt = true
		case 0x20:
			target = &p.MaxDatagramFrameSize
		case 0x3e:
			p.InitialMaxPathID = new(uint64)
			target = p.InitialMaxPathID
		case 0x2ab2:
			if len(value) != 0 {
				return fail(id, "grease_quic_bit must be empty")
			}
			p.GreaseQUICBit = true
		}
		if target != nil {
			v, n, err := ConsumeVarint(value)
			if err != nil || n != len(value) {
				return fail(id, "integer must occupy the entire parameter value")
			}
			*target = v
		}
	}
	if err := p.Validate(senderIsServer); err != nil {
		return TransportParameters{}, err
	}
	return p, nil
}

func appendParameter(b []byte, id uint64, value []byte) []byte {
	b, _ = AppendVarint(b, id)
	b, _ = AppendVarint(b, uint64(len(value)))
	return append(b, value...)
}

// Marshal emits parameters in a stable order and omits numeric defaults. It
// never omits present empty CIDs or confuses zero with nonzero wire defaults.
func (p *TransportParameters) Marshal(senderIsServer bool) ([]byte, error) {
	if err := p.Validate(senderIsServer); err != nil {
		return nil, err
	}
	var b []byte
	if p.OriginalDestinationConnectionID != nil {
		b = appendParameter(b, 0, p.OriginalDestinationConnectionID)
	}
	for _, n := range p.numericParameters() {
		if n.value != n.def {
			value, _ := AppendVarint(nil, n.value)
			b = appendParameter(b, n.id, value)
		}
	}
	if p.StatelessResetToken != nil {
		b = appendParameter(b, 2, p.StatelessResetToken[:])
	}
	if p.DisableActiveMigration {
		b = appendParameter(b, 12, nil)
	}
	if a := p.PreferredAddress; a != nil {
		value := append([]byte(nil), a.IPv4[:]...)
		value = binary.BigEndian.AppendUint16(value, a.IPv4Port)
		value = append(value, a.IPv6[:]...)
		value = binary.BigEndian.AppendUint16(value, a.IPv6Port)
		value = append(value, byte(len(a.ConnectionID)))
		value = append(value, a.ConnectionID...)
		value = append(value, a.StatelessResetToken[:]...)
		b = appendParameter(b, 13, value)
	}
	b = appendParameter(b, 15, p.InitialSourceConnectionID)
	if p.RetrySourceConnectionID != nil {
		b = appendParameter(b, 16, p.RetrySourceConnectionID)
	}
	if p.VersionInformation != nil {
		b = appendParameter(b, 0x11, p.VersionInformation.marshal())
	}
	if p.InitialMaxPathID != nil {
		value, _ := AppendVarint(nil, *p.InitialMaxPathID)
		b = appendParameter(b, 0x3e, value)
	}
	if p.ResetStreamAt {
		b = appendParameter(b, 0x1d, nil)
	}
	if p.GreaseQUICBit {
		b = appendParameter(b, 0x2ab2, nil)
	}
	return b, nil
}
