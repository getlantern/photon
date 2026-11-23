package quic

import "github.com/getlantern/photon/internal/handshake"

// SupportedVersions returns supported wire versions in default preference order.
// Each call returns an independent slice; changing it does not change defaults.
func SupportedVersions() []Version { return []Version{Version1, Version2} }

// QUICVersionContextKey identifies the Version at TLS handshake startup in
// tls.ClientHelloInfo.Context. Compatible negotiation can subsequently select a
// different version; Conn.ConnectionState reports the negotiated version.
var QUICVersionContextKey = handshake.QUICVersionContextKey
