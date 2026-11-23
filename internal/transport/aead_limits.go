package transport

import "github.com/getlantern/photon/internal/packet"

func aeadLimitError() *TransportError {
	return &TransportError{Code: 0xf, Reason: "AEAD usage limit reached"}
}
func (c *Conn) checkIntegrityLimit(keys *packet.Keys) error {
	_, limit := keys.UsageLimits()
	if c.integrityLimit == 0 || limit < c.integrityLimit {
		c.integrityLimit = limit
	}
	if c.failedAuth >= c.integrityLimit {
		return c.fail(aeadLimitError())
	}
	return nil
}
