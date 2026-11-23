package transport

import "github.com/getlantern/photon/internal/wire"

// RFC 9000 section 12.3 requires immediate silent closure at 2^62-1:
// neither application data nor CONNECTION_CLOSE may use that number.
func (c *Conn) exhaustPacketNumbers() error {
	err := c.fail(wire.ErrPacketNumber)
	c.Close()
	return err
}
