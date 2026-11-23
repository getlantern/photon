package wire

import (
	"errors"
	"strconv"
)

type Version uint32

const (
	Version1 Version = 0x00000001
	Version2 Version = 0x6b3343cf
)

var ErrVersion = errors.New("quic: unsupported version")

// PacketType is independent of the version-specific long-header type bits.
type PacketType uint8

const (
	Initial PacketType = iota
	ZeroRTT
	Handshake
	Retry
	// OneRTT uses a short header and has no long-header type bits.
	OneRTT
)

func (v Version) Supported() bool { return v == Version1 || v == Version2 }

func (v Version) TypeBits(t PacketType) (byte, error) {
	if !v.Supported() {
		return 0, ErrVersion
	}
	if t > Retry {
		return 0, errors.New("quic: invalid packet type")
	}
	if v == Version2 {
		return byte((t + 1) % 4), nil
	}
	return byte(t), nil
}

func (v Version) PacketType(first byte) (PacketType, error) {
	if !v.Supported() {
		return 0, ErrVersion
	}
	t := PacketType((first >> 4) & 3)
	if v == Version2 {
		t = (t + 3) % 4
	}
	return t, nil
}

// String names known historical versions as well as supported versions. A name
// does not imply wire support. Other values use unpadded lowercase hexadecimal.
func (v Version) String() string {
	if v == Version1 {
		return "v1"
	}
	if v == Version2 {
		return "v2"
	}
	if v == 0xffffffff {
		return "unknown"
	}
	if v == 0xff00001d {
		return "draft-29"
	}
	// Preserve the reference formatter's historical Q000–Q049 numeric range,
	// including the non-ASCII values between its endpoints.
	if v > 0x51303030 && v <= 0x51303439 {
		n := int((uint32(v)-0x51303030)*10/256) + int(uint32(v)&15)
		return "gQUIC " + strconv.Itoa(n)
	}
	return "0x" + strconv.FormatUint(uint64(v), 16)
}
