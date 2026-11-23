package transport

import (
	"crypto/rand"
	"github.com/getlantern/photon/internal/wire"
)

// Local advertisement grants receive permission; peer advertisement only grants
// permission to vary the bit when sending. Never normalize authenticated bytes.
func (c *Conn) parseHeader(data []byte) (wire.Header, error) {
	return wire.ParseHeaderWithQUICBit(data, len(c.config.LocalCID), c.config.Parameters.GreaseQUICBit)
}

// Packet builders choose the bit before AEAD sealing. Cached cryptographic bits
// avoid a random-source call for every packet. No remembered permission is used.
func (c *Conn) greaseQUICBit(first byte) byte {
	if c.peerParams == nil || !c.peerParams.GreaseQUICBit {
		return first
	}
	if c.quicBitsLeft == 0 {
		rand.Read(c.quicBits[:]) // Go 1.26 crypto/rand.Read fills the buffer or terminates.
		c.quicBitsLeft = uint16(len(c.quicBits) * 8)
	}
	c.quicBitsLeft--
	index := c.quicBitsLeft
	bit := (c.quicBits[index/8] >> (index % 8)) & 1
	return first&^0x40 | bit<<6
}
