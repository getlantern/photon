package transport

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"net"
	"net/netip"

	"github.com/getlantern/photon/internal/wire"
)

// VersionNegotiationError reports an unauthenticated rejection of the selected
// version. It is not proof of the peer's supported versions. A subsequent
// attempt must validate the authenticated peer version information.
type VersionNegotiationError struct {
	Ours          []wire.Version
	Theirs        []wire.Version
	ChosenVersion wire.Version
	PeerVersions  []wire.Version
}

func (*VersionNegotiationError) Error() string { return "quic: peer rejected the selected version" }
func (*VersionNegotiationError) Unwrap() error { return net.ErrClosed }

func (e *ServerEndpoint) negotiateVersion(data []byte, peer netip.AddrPort) (*Outbound, error) {
	if e.DisableVersionNegotiationPackets || len(data) < 1200 || len(data) > 65527 || !peer.IsValid() || peer.Port() == 0 {
		return nil, nil
	}
	h, _, err := wire.ParseInvariantLongHeader(data)
	if err != nil || h.Version == 0 {
		return nil, nil
	}
	if (e.config.Version == 0 && h.Version.Supported()) || h.Version == e.config.Version {
		return nil, nil
	}
	versions := []wire.Version{wire.Version1, wire.Version2}
	if e.config.Version != 0 {
		versions = []wire.Version{e.config.Version}
	}
	var random [5]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, err
	}
	// Exercise tolerance of reserved versions without claiming to support one.
	grease := wire.Version(binary.BigEndian.Uint32(random[1:])&0xf0f0f0f0 | 0x0a0a0a0a)
	versions = append(versions, grease)
	response, err := wire.AppendVersionNegotiation(nil, random[0]|0x40, h.SCID, h.DCID, versions)
	if err != nil {
		return nil, err
	}
	return &Outbound{Peer: peer, Data: response}, nil
}

func (c *Conn) handleVersionNegotiation(h wire.Header) error {
	if c.config.Negotiation != nil || c.config.Server || c.authenticatedPackets != 0 || c.retryUsed || !c.spaces[initialSpace].sentAny || !bytes.Equal(h.DCID, c.config.LocalCID) || !bytes.Equal(h.SCID, c.config.OriginalDCID) {
		return nil
	}
	for _, v := range h.Versions {
		if v == c.config.Version {
			return nil
		}
	}
	err := c.fail(&VersionNegotiationError{
		Ours: []wire.Version{c.config.Version}, Theirs: append([]wire.Version(nil), h.Versions...),
		ChosenVersion: c.config.Version, PeerVersions: append([]wire.Version(nil), h.Versions...),
	})
	c.Close() // No close packet is sent in response to unauthenticated negotiation.
	return err
}

// VersionNegotiationState records the original attempt and the exact preference
// policy used to select a fallback. Conn.originalVersion is that attempt's
// chosen version; Config.Version can change during compatible negotiation.
type VersionNegotiationState struct {
	Original    wire.Version
	Preferences []wire.Version
}

func (c *Conn) validateVersionInformation(v *wire.VersionInformation) error {
	reject := func(reason string) error { return &TransportError{Code: 0x11, Reason: reason} }
	history := c.config.Negotiation
	if v == nil && history != nil {
		if c.config.Version != wire.Version1 {
			return reject("missing version information after negotiation")
		}
		// RFC 9368 section 8's legacy v1 exception still undergoes all checks.
		v = &wire.VersionInformation{Chosen: wire.Version1, Available: []wire.Version{wire.Version1}}
	}
	expected := c.config.Version
	if c.config.Server && c.originalVersion != 0 {
		expected = c.originalVersion
	}
	if v != nil && v.Chosen != expected {
		return reject("chosen version differs from packet version")
	}
	if history == nil {
		return nil
	}
	if len(v.Available) == 0 {
		return reject("empty available versions after negotiation")
	}
	attempted := c.originalVersion
	if attempted == 0 {
		attempted = c.config.Version
	}
	// Replay the original selection policy using the authenticated deployment
	// list plus the negotiated version, which can be undergoing removal.
	for _, preferred := range history.Preferences {
		available := preferred == c.config.Version
		for _, offered := range v.Available {
			if preferred == offered {
				available = true
				break
			}
		}
		if available {
			if preferred != attempted {
				return reject("version negotiation changed client preference")
			}
			return nil
		}
	}
	return reject("no supported authenticated version")
}
