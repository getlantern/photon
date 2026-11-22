package handshake

import (
	"crypto/tls"
	"errors"

	"github.com/getlantern/photon/internal/wire"
)

// VersionSelector runs before TLS traffic keys are derived or flights exposed.
// It returns the negotiated version and server transport parameters. The caller
// still owns packet-version, CID, Retry, and downgrade validation in transport.
type VersionSelector func(wire.TransportParameters) (wire.Version, []byte, error)

// NewNegotiatingServer defers server transport parameters until the client's
// parameters have been parsed. Ticket acceptance remains bound to the original
// version; newly issued tickets are bound to the negotiated version.
func NewNegotiatingServer(original wire.Version, config *tls.Config, localParams []byte, selectVersion VersionSelector) (*TLS, error) {
	if selectVersion == nil {
		return nil, errors.New("quic: version selector is required")
	}
	return newTLS(true, original, config, localParams, selectVersion)
}

func versionOffered(info *wire.VersionInformation, original, selected wire.Version) bool {
	if !selected.Supported() || !original.Supported() {
		return false
	}
	if info == nil {
		return selected == original
	}
	if info.Chosen != original {
		return false
	}
	for _, v := range info.Available {
		if v == selected {
			return true
		}
	}
	return false
}

func (h *TLS) selectServerVersion(peer wire.TransportParameters) error {
	v, encoded, err := h.selectVersion(peer)
	if err != nil {
		return err
	}
	if h.versionSelected || !versionOffered(peer.VersionInformation, h.version, v) {
		return errors.New("quic: invalid compatible version selection")
	}
	params, err := wire.ParseTransportParameters(encoded, true)
	if err != nil {
		return err
	}
	if params.VersionInformation == nil || params.VersionInformation.Chosen != v {
		return errors.New("quic: selected version differs from server parameters")
	}
	h.pendingParams = append([]byte{}, encoded...)
	h.localTransport = params.EarlyTransport()
	h.version, h.versionSelected = v, true
	return nil
}

// SetNegotiatedVersion applies the client's authenticated Initial version choice
// before feeding server CRYPTO to TLS. It cannot replace Handshake or application
// keys; existing early keys remain tied to the original version.
// The transport must authenticate the packet and enforce a single version choice.
func (h *TLS) SetNegotiatedVersion(v wire.Version) error {
	if h.server || !h.started || h.closed || h.fatal != nil || h.versionSelected || h.gotParams || len(h.read) != 0 || h.hasNonEarlyWriteKeys() || !versionOffered(h.localVersions, h.version, v) {
		return errors.New("quic: invalid TLS version transition")
	}
	h.version, h.versionSelected = v, true
	return nil
}

func (h *TLS) Version() wire.Version { return h.version }

func (h *TLS) hasNonEarlyWriteKeys() bool {
	for level := range h.write {
		if level != tls.QUICEncryptionLevelEarly {
			return true
		}
	}
	return false
}

// NewNegotiatingServerWithEarlyData retains original-version 0-RTT keys while
// negotiating the version used for Handshake and application protection.
func NewNegotiatingServerWithEarlyData(original wire.Version, config *tls.Config, localParams []byte, selectVersion VersionSelector) (*TLS, error) {
	if selectVersion == nil {
		return nil, errors.New("quic: version selector is required")
	}
	return newTLSWithEarlyData(true, original, config, localParams, selectVersion, true)
}
