package transport

import "github.com/getlantern/photon/internal/wire"

// A peer can advertise multipath even when we do not. That does not negotiate
// the extension. The draft's zero-length CID prohibition still applies to the
// advertisement itself. Handshake packets use the initial peer and local CIDs.
func (c *Conn) validatePeerMultipathParameters(p wire.TransportParameters) error {
	if p.InitialMaxPathID != nil && (len(c.initialPeerCID) == 0 || len(c.config.LocalCID) == 0) {
		return &TransportError{Code: 0xa, Reason: "multipath advertisement with zero-length connection ID"}
	}
	return nil
}
