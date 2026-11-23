package transport

// receiveAccounting counts routed network arrivals, not successful decryption
// or frame processing. An unparseable remainder counts as one packet attempt.
// Deferred processing calls the packet receiver directly and never enters here.
type receiveAccounting struct {
	packets, bytes uint64
}

func (c *Conn) recordReceivedDatagram(data []byte) {
	c.receivedTotals.bytes += uint64(len(data))
	for len(data) > 0 {
		c.receivedTotals.packets++
		// A short-header packet consumes the rest of its enclosing datagram.
		// This also avoids a second parse on the established-data hot path.
		if data[0]&0x80 == 0 {
			return
		}
		h, err := c.parseHeader(data)
		if err != nil || h.PacketLen <= 0 || h.PacketLen > len(data) {
			return
		}
		data = data[h.PacketLen:]
	}
}
