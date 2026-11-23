package transport

import (
	"github.com/getlantern/photon/internal/packet"
	"github.com/getlantern/photon/internal/wire"
)

// Only one alternative can be pending: supported compatible versions are v1/v2.
// Retry invalidates this cache because it changes the Initial key DCID.
type candidateInitialKeys struct {
	version     wire.Version
	read, write *packet.Keys
}

func (c *Conn) candidateInitialRead(v wire.Version) (*packet.Keys, error) {
	if c.candidateInitial.version != v || c.candidateInitial.read == nil {
		write, read, err := packet.InitialKeys(v, c.initialKeyDCID())
		if err != nil {
			return nil, err
		}
		c.candidateInitial = candidateInitialKeys{version: v, read: read, write: write}
	}
	return c.candidateInitial.read, nil
}

func (c *Conn) initialKeyDCID() []byte {
	if c.config.RetrySourceCID != nil {
		return c.config.RetrySourceCID
	}
	if c.retryUsed {
		return c.retrySourceCID
	}
	return c.config.OriginalDCID
}

func (c *Conn) acceptPacketVersion(h wire.Header) bool {
	if h.Long && h.Type == wire.ZeroRTT {
		return c.config.Server && c.config.enableEarlyData && h.Version == c.originalVersion
	}
	if !h.Long || h.Version == c.config.Version {
		return true
	}
	if h.Type != wire.Initial || c.spaces[initialSpace].discarded {
		return false
	}
	if c.config.Server {
		return h.Version == c.originalVersion && c.originalInitialRead != nil
	}
	if c.versionChosen || len(c.config.CompatibleVersions) == 0 || !h.Version.Supported() {
		return false
	}
	for _, v := range c.config.Parameters.VersionInformation.Available {
		if v == h.Version {
			return true
		}
	}
	return false
}

// Keeps packet number spaces, recovery, and CRYPTO offsets in the same connection.
func (c *Conn) switchVersion(v wire.Version) error {
	if c.config.Server {
		read, write, err := packet.InitialKeys(v, c.initialKeyDCID())
		if err != nil {
			return err
		}
		c.originalInitialRead = c.initialRead
		c.initialRead, c.initialWrite = read, write
	} else {
		if _, err := c.candidateInitialRead(v); err != nil {
			return err
		}
		c.initialRead, c.initialWrite = c.candidateInitial.read, c.candidateInitial.write
		c.candidateInitial = candidateInitialKeys{}
	}
	c.config.Version, c.versionChosen = v, true
	return nil
}

func (c *Conn) selectCompatibleVersion(peer wire.TransportParameters) (wire.Version, []byte, error) {
	if err := c.validateVersionInformation(peer.VersionInformation); err != nil {
		return 0, nil, err
	}
	ids := wire.ConnectionIDContext{PeerIsServer: false, InitialSourceConnectionID: c.initialPeerCID}
	if err := peer.ValidateConnectionIDs(ids); err != nil {
		return 0, nil, err
	}
	chosen := c.config.Version
	if peer.VersionInformation != nil {
	selection:
		for _, preferred := range c.config.CompatibleVersions {
			for _, offered := range peer.VersionInformation.Available {
				if preferred == offered {
					chosen = preferred
					break selection
				}
			}
		}
	}
	params := c.config.Parameters
	params.VersionInformation = &wire.VersionInformation{Chosen: chosen, Available: append([]wire.Version(nil), c.config.CompatibleVersions...)}
	encoded, err := params.Marshal(true)
	if err != nil {
		return 0, nil, err
	}
	if chosen != c.config.Version {
		if err := c.switchVersion(chosen); err != nil {
			return 0, nil, err
		}
	} else {
		c.versionChosen = true
	}
	c.config.Parameters = params
	return chosen, encoded, nil
}
