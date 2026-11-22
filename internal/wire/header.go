package wire

import (
	"encoding/binary"
	"errors"
	"io"
)

var ErrHeader = errors.New("quic: invalid packet header")

// Header describes the unprotected fields. Slices borrow the input datagram.
// PacketLen separates a long-header packet from subsequent coalesced packets.
// PNOffset is zero for Retry and Version Negotiation packets.
type Header struct {
	Long                bool
	Version             Version
	Type                PacketType
	DCID, SCID          []byte
	Token               []byte
	Versions            []Version
	PNOffset, PacketLen int
}

// ParseHeader parses invariant and unprotected fields only. Protected flags,
// packet numbers, and payload are processed after selecting packet keys.
// shortCIDLen comes from the local connection ID configuration.
func ParseHeader(b []byte, shortCIDLen int) (Header, error) {
	return ParseHeaderWithQUICBit(b, shortCIDLen, false)
}

// ParseHeaderWithQUICBit permits a cleared QUIC bit only when allowZero is true.
// The receive owner must choose this policy from its advertised RFC 9287 support,
// not from the peer's advertisement (which grants sending permission). Parsing
// never changes the input: the original bit remains authenticated packet data.
// Version Negotiation and unknown-version invariant handling are unchanged.
func ParseHeaderWithQUICBit(b []byte, shortCIDLen int, allowZero bool) (Header, error) {
	var h Header
	if len(b) == 0 {
		return h, io.ErrUnexpectedEOF
	}
	h.Long = b[0]&0x80 != 0
	if !h.Long {
		h.Type = OneRTT
		if shortCIDLen < 0 || shortCIDLen > 20 || !allowZero && b[0]&0x40 == 0 {
			return h, ErrHeader
		}
		if len(b) < 1+shortCIDLen {
			return h, io.ErrUnexpectedEOF
		}
		h.DCID, h.PNOffset, h.PacketLen = b[1:1+shortCIDLen], 1+shortCIDLen, len(b)
		return h, nil
	}
	var i int
	var err error
	h, i, err = ParseInvariantLongHeader(b)
	if err != nil {
		return h, err
	}
	if h.Version == 0 {
		if len(b)-i == 0 || (len(b)-i)%4 != 0 {
			return h, ErrHeader
		}
		for ; i < len(b); i += 4 {
			h.Versions = append(h.Versions, Version(binary.BigEndian.Uint32(b[i:i+4])))
		}
		h.PacketLen = len(b)
		return h, nil
	}
	// Unknown versions may change everything after the invariant CID fields.
	if !h.Version.Supported() {
		return h, ErrVersion
	}
	if !allowZero && b[0]&0x40 == 0 || len(h.DCID) > 20 || len(h.SCID) > 20 {
		return h, ErrHeader
	}
	h.Type, _ = h.Version.PacketType(b[0])
	if h.Type == Retry {
		if len(b)-i < 17 {
			return h, ErrHeader
		} // Nonempty token and 16-byte tag.
		h.Token, h.PacketLen = b[i:len(b)-16], len(b)
		return h, nil
	}
	if h.Type == Initial {
		length, consumed, err := ConsumeVarint(b[i:])
		if err != nil {
			return h, err
		}
		i += consumed
		if length > uint64(len(b)-i) {
			return h, io.ErrUnexpectedEOF
		}
		h.Token = b[i : i+int(length)]
		i += int(length)
	}
	length, consumed, err := ConsumeVarint(b[i:])
	if err != nil {
		return h, err
	}
	i += consumed
	if length > uint64(len(b)-i) {
		return h, io.ErrUnexpectedEOF
	}
	if length < 1 {
		return h, ErrHeader
	}
	h.PNOffset, h.PacketLen = i, i+int(length)
	return h, nil
}

// AppendLongHeader appends an Initial, Handshake, or 0-RTT header including
// the packet number. payloadLen includes the AEAD tag, but not the packet number.
func AppendLongHeader(dst []byte, v Version, t PacketType, dcid, scid, token []byte, pn uint64, pnLen, payloadLen int) ([]byte, error) {
	bits, err := v.TypeBits(t)
	if err != nil {
		return dst, err
	}
	if t == Retry || len(dcid) > 20 || len(scid) > 20 || (t != Initial && len(token) != 0) || pn > MaxVarint || pnLen < 1 || pnLen > 4 || payloadLen < 0 || uint64(payloadLen) > MaxVarint-uint64(pnLen) {
		return dst, ErrHeader
	}
	dst = append(dst, 0xc0|bits<<4|byte(pnLen-1))
	dst = binary.BigEndian.AppendUint32(dst, uint32(v))
	dst = append(dst, byte(len(dcid)))
	dst = append(dst, dcid...)
	dst = append(dst, byte(len(scid)))
	dst = append(dst, scid...)
	if t == Initial {
		dst, _ = AppendVarint(dst, uint64(len(token)))
		dst = append(dst, token...)
	}
	dst, _ = AppendVarint(dst, uint64(payloadLen)+uint64(pnLen))
	for i := pnLen - 1; i >= 0; i-- {
		dst = append(dst, byte(pn>>(8*i)))
	}
	return dst, nil
}

// ParseInvariantLongHeader reads only version-independent long-header fields.
// CID lengths may be up to 255; version-specific limits belong to ParseHeader.
func ParseInvariantLongHeader(b []byte) (Header, int, error) {
	h := Header{Long: true}
	if len(b) == 0 {
		return h, 0, io.ErrUnexpectedEOF
	}
	if b[0]&0x80 == 0 {
		return h, 0, ErrHeader
	}
	if len(b) < 6 {
		return h, 0, io.ErrUnexpectedEOF
	}
	h.Version = Version(binary.BigEndian.Uint32(b[1:5]))
	i := 6
	n := int(b[5])
	if n > len(b)-i {
		return h, 0, io.ErrUnexpectedEOF
	}
	h.DCID = b[i : i+n]
	i += n
	if i == len(b) {
		return h, 0, io.ErrUnexpectedEOF
	}
	n = int(b[i])
	i++
	if n > len(b)-i {
		return h, 0, io.ErrUnexpectedEOF
	}
	h.SCID = b[i : i+n]
	i += n
	return h, i, nil
}
