package quic

import (
	"github.com/getlantern/photon/internal/recovery"

	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/getlantern/photon/internal/transport"
	"github.com/getlantern/photon/internal/wire"
)

// DialAddr establishes a certificate-verified QUIC connection on its own UDP
// socket. TLS.ServerName defaults to the address host. The context bounds DNS
// resolution and handshake; canceling it after success does not close the Conn.
// Context values are retained by the connection and its streams.
// The first resolved address is used. Configured versions are tried in preference
// order after valid Version Negotiation, with one authenticated fallback attempt.
// Default preference is v1, then v2. Transport.Dial uses a shared UDP socket.
func DialAddr(ctx context.Context, addr string, tlsConfig *tls.Config, config *Config) (*Conn, error) {
	return dialAddr(ctx, addr, tlsConfig, config, false)
}

// DialAddrEarly may return before handshake completion when a cached TLS ticket
// permits 0-RTT. Early data can be replayed by the network. Rejected streams return
// Err0RTTRejected; requests are never replayed automatically. Without a usable
// ticket this waits for an established connection.
func DialAddrEarly(ctx context.Context, addr string, tlsConfig *tls.Config, config *Config) (*Conn, error) {
	return dialAddr(ctx, addr, tlsConfig, config, true)
}

func dialAddr(ctx context.Context, addr string, tlsConfig *tls.Config, config *Config, early bool) (*Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cfg, params, err := prepareConfig(config)
	cfg.earlyDial = early
	if err != nil {
		return nil, err
	}
	if err := validateTLS(tlsConfig); err != nil {
		return nil, err
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	if host == "" {
		return nil, errors.New("quic: empty peer host")
	}
	service, err := net.DefaultResolver.LookupPort(ctx, "udp", port)
	if err != nil {
		return nil, err
	}
	if service == 0 {
		return nil, errors.New("quic: zero peer port")
	}
	var ip netip.Addr
	if parsed, err := netip.ParseAddr(host); err == nil {
		ip = parsed
	} else {
		ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if err != nil {
			return nil, err
		}
		if len(ips) == 0 {
			return nil, errors.New("quic: no peer addresses")
		}
		ip = ips[0]
	}
	ip = ip.Unmap()
	peer := netip.AddrPortFrom(ip, uint16(service))
	tlsCopy := tlsConfig.Clone()
	if tlsCopy.ServerName == "" {
		tlsCopy.ServerName = host
	}
	return dialPrepared(ctx, peer, tlsCopy, cfg, params, nil)
}

func dialPrepared(ctx context.Context, peer netip.AddrPort, tlsCopy *tls.Config, cfg Config, params wire.TransportParameters, runtime *transport.UDPServer) (*Conn, error) {
	versions := append([]Version(nil), cfg.Versions...)
	if len(versions) == 0 {
		versions = []Version{Version1, Version2}
	}
	initial := cfg.InitialVersion
	if initial == 0 {
		initial = versions[0]
	}
	c, err := dialAttempt(ctx, peer, tlsCopy, cfg, params, initial, nil, runtime)
	var rejected *transport.VersionNegotiationError
	if !errors.As(err, &rejected) {
		return c, err
	}
	for _, preferred := range versions {
		for _, offered := range rejected.PeerVersions {
			if preferred == offered {
				history := &transport.VersionNegotiationState{Original: initial, Preferences: versions}
				return dialAttempt(ctx, peer, tlsCopy, cfg, params, preferred, history, runtime)
			}
		}
	}
	// The packet engine knows this attempt's version. Report the complete local
	// preference list when no offered version permits another attempt, without
	// mutating the terminal error retained by the closed connection.
	result := *rejected
	result.Ours = append([]Version(nil), versions...)
	return nil, &result
}

func dialAttempt(ctx context.Context, peer netip.AddrPort, tlsConfig *tls.Config, cfg Config, params wire.TransportParameters, version Version, history *transport.VersionNegotiationState, runtime *transport.UDPServer) (*Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var generate func() ([]byte, error)
	var resetKey *transport.ResetKey
	cidLength := 8
	if runtime != nil {
		if err := runtime.Do(ctx, func(e *transport.ServerEndpoint) error {
			generate = e.GenerateConnectionID
			resetKey = e.ResetKey
			cidLength = e.ConnectionIDLength()
			return nil
		}); err != nil {
			return nil, err
		}
	}
	localCID, dcid := make([]byte, cidLength), make([]byte, 8)
	if generate != nil {
		var err error
		localCID, err = generate()
		if err != nil {
			return nil, err
		}
	} else if _, err := rand.Read(localCID); err != nil {
		return nil, err
	}
	if _, err := rand.Read(dcid); err != nil {
		return nil, err
	}
	var raw *transport.Conn
	var initialToken []byte
	var onToken func([]byte, time.Time)
	if cfg.TokenStore != nil {
		key := clientTokenKey(tlsConfig.ServerName, peer.Port(), version)
		if token := cfg.TokenStore.Pop(key); token != nil && token.version == version && tokenCurrent(token.received, time.Now()) && len(token.data) <= maxClientTokenSize {
			initialToken = append([]byte(nil), token.data...)
		}
		onToken = func(data []byte, received time.Time) {
			issuedVersion := raw.Version() // Callback runs on the connection owner.
			issuedKey := clientTokenKey(tlsConfig.ServerName, peer.Port(), issuedVersion)
			cfg.TokenStore.Put(issuedKey, &ClientToken{data: data, version: issuedVersion, received: received})
		}
	}
	compatible := compatibleVersions(cfg)
	if len(compatible) != 0 {
		params.VersionInformation = &wire.VersionInformation{Chosen: version, Available: compatible}
	}
	raw, err := transport.NewConn(transport.Config{GenerateConnectionID: generate, ResetKey: resetKey, MaxStreamReceiveWindow: cfg.MaxStreamReceiveWindow, MaxConnectionReceiveWindow: cfg.MaxConnectionReceiveWindow, EnableEarlyData: cfg.earlyDial, CompatibleVersions: compatible, InitialToken: initialToken, OnToken: onToken, DisablePathMTUDiscovery: cfg.DisablePathMTUDiscovery, CongestionControl: recovery.Algorithm(cfg.CongestionControl), InitialPacketSize: cfg.InitialPacketSize, EnableMigration: true, EnableMultipath: cfg.EnableMultipath, RequireStreamCreditRelease: cfg.RequireStreamCreditRelease, Negotiation: history, Version: version, TLS: tlsConfig, LocalCID: localCID, PeerCID: dcid, OriginalDCID: dcid, Parameters: params, HandshakeTimeout: cfg.HandshakeTimeout, HandshakeIdleTimeout: cfg.HandshakeIdleTimeout, KeepAlivePeriod: cfg.KeepAlivePeriod})
	if err != nil {
		return nil, err
	}
	handshakeContext := ctx
	if cfg.earlyDial {
		handshakeContext = context.WithoutCancel(ctx)
	}
	if err := raw.Start(handshakeContext); err != nil {
		raw.Close()
		return nil, err
	}
	shared := runtime != nil
	if shared {
		if err := runtime.Do(ctx, func(e *transport.ServerEndpoint) error { return e.AddClient(raw, peer) }); err != nil {
			raw.Close()
			return nil, err
		}
	} else {
		endpoint, err := transport.NewClientEndpoint(raw, peer)
		if err != nil {
			raw.Close()
			return nil, err
		}
		network := "udp6"
		if peer.Addr().Is4() {
			network = "udp4"
		}
		if cfg.EnableMultipath && (cfg.LocalAddr == nil || cfg.LocalAddr.IP.IsUnspecified() || cfg.LocalAddr.IP.To16() == nil || cfg.LocalAddr.IP.IsMulticast()) {
			endpoint.Close()
			return nil, errors.New("quic: multipath requires a concrete Config.LocalAddr")
		}
		socket, err := net.ListenUDP(network, cfg.LocalAddr)
		if err != nil {
			endpoint.Close()
			return nil, err
		}
		runtime, err = transport.NewUDPServer(socket, endpoint)
		if err != nil {
			socket.Close()
			endpoint.Close()
			return nil, err
		}
	}
	success := false
	defer func() {
		if !success {
			if shared {
				_ = runtime.Do(context.Background(), func(e *transport.ServerEndpoint) error { e.Remove(raw); return nil })
			} else {
				runtime.Close()
			}
		}
	}()
	lifetime, err := runtime.ConnectionContext(context.Background(), raw)
	if err != nil {
		return nil, err
	}
	err = runtime.Wait(ctx, func(*transport.ServerEndpoint) (bool, error) {
		if err := raw.Err(); err != nil {
			return true, err
		}
		return raw.HandshakeConfirmed() || (cfg.earlyDial && raw.EarlyReady()), nil
	})
	if err != nil {
		return nil, err
	}
	c := &Conn{server: runtime, raw: raw, ctx: lifetime, local: runtime.Addr(), remote: net.UDPAddrFromAddrPort(peer), datagrams: cfg.EnableDatagrams}
	if err := runtime.Do(context.Background(), func(e *transport.ServerEndpoint) error {
		if err := raw.Err(); err != nil {
			return err
		}
		c.installReceiveWindowPolicy(cfg.AllowConnectionWindowIncrease)
		if err := c.observeHandshake(); err != nil {
			return err
		}
		if !shared {
			e.CloseWhenIdle()
		}
		return nil
	}); err != nil {
		return nil, err
	}
	success = true
	return c, nil
}

// A token belongs to one authenticated server authority and QUIC version.
// Port scoping deliberately avoids sharing evidence between separate services.
func clientTokenKey(serverName string, port uint16, version Version) string {
	return fmt.Sprintf("%08x:%s", version, net.JoinHostPort(strings.ToLower(serverName), strconv.Itoa(int(port))))
}
