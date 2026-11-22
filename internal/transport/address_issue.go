package transport

import (
	"errors"
	"net/netip"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

// IssueAddressToken queues one fresh NEW_TOKEN after server confirmation. The
// endpoint must supply the actual validated client address. A fresh call creates
// a distinct token; recovery retains and retransmits the original frame bytes.
// This is not an application authentication or early-data acceptance decision.
func (c *Conn) IssueAddressToken(address netip.AddrPort, now time.Time) error {
	if err := c.check(); err != nil {
		return err
	}
	if !c.config.Server {
		return errors.New("quic: only servers issue address tokens")
	}
	if !c.confirmed {
		return ErrHandshakeUnconfirmed
	}
	if c.config.AddressTokens == nil {
		return errors.New("quic: address token issuance disabled")
	}
	opaque, err := c.config.AddressTokens.Issue(address, c.config.Version, now)
	if err != nil {
		return err
	}
	size := len(opaque) + 16
	// Optional token issuance must not close a connection whose send queue is full.
	if size > c.availableSendBytes() {
		return ErrSendBuffer
	}
	return c.enqueue(applicationSpace, wire.NewTokenFrame{Token: opaque}, size)
}
