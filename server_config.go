package quic

import (
	"errors"
	"net"
	"net/netip"
	"slices"

	"github.com/getlantern/photon/internal/transport"
)

// Only the policy needed by an accepted public handle survives admission.
type acceptedConfig struct {
	datagrams           bool
	allowWindowIncrease func(*Conn, uint64) bool
}

func serverConfigSelector(listener Config) func(transport.Config, netip.AddrPort) (transport.Config, error) {
	selectConfig := listener.GetConfigForClient
	preferred := slices.Clone(listener.PreferredAddresses)
	return func(base transport.Config, peer netip.AddrPort) (transport.Config, error) {
		selected, err := selectConfig(&ClientInfo{RemoteAddr: net.UDPAddrFromAddrPort(peer), AddrVerified: base.AddressVerified})
		if err != nil {
			return base, err
		}
		cfg, params, err := prepareConfig(selected)
		if err != nil {
			return base, err
		}
		if cfg.EnableMultipath && !listener.EnableMultipath {
			return base, errors.New("quic: selected multipath requires listener support")
		}
		if cfg.EnableMultipath && cfg.Allow0RTT {
			return base, errors.New("quic: early data with multipath is not integrated")
		}
		if len(cfg.PreferredAddresses) != 0 && !slices.Equal(cfg.PreferredAddresses, preferred) {
			return base, errors.New("quic: selected preferred addresses require listener sockets")
		}
		// Version negotiation and socket registration have already happened. Keep
		// their identity/capabilities and apply the connection-specific settings.
		params.VersionInformation = base.Parameters.VersionInformation
		base.Parameters = params
		base.MaxStreamReceiveWindow = cfg.MaxStreamReceiveWindow
		base.MaxConnectionReceiveWindow = cfg.MaxConnectionReceiveWindow
		base.EnableEarlyData = cfg.Allow0RTT
		base.EnableMultipath = cfg.EnableMultipath
		base.RequireStreamCreditRelease = cfg.RequireStreamCreditRelease
		base.DisablePathMTUDiscovery = cfg.DisablePathMTUDiscovery
		base.CongestionControl = cfg.congestionAlgorithm()
		base.CongestionStates = cfg.congestionStates()
		base.CarefulResumeMaxJump = cfg.carefulResumeMaxJump()
		base.InitialPacketSize = cfg.InitialPacketSize
		base.HandshakeTimeout = cfg.HandshakeTimeout
		base.HandshakeIdleTimeout = cfg.HandshakeIdleTimeout
		base.KeepAlivePeriod = cfg.KeepAlivePeriod
		base.AdapterData = acceptedConfig{cfg.EnableDatagrams, cfg.AllowConnectionWindowIncrease}
		return base, nil
	}
}
