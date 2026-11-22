package http3

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"

	quic "github.com/getlantern/photon"
	h3 "github.com/getlantern/photon/internal/http3wire"
)

// Dial establishes a QUIC connection and initializes HTTP/3 for one HTTPS origin.
// ctx bounds DNS, handshake and HTTP/3 initialization; canceling it after success
// does not close the returned client. Close the client explicitly. TLS settings
// are cloned, ALPN is set to h3, and an empty ServerName defaults to the origin
// host before QUIC dialing. Nil TLS configuration uses system roots and normal
// certificate verification. Explicit TLS verification policy is preserved.
// There is no pooling, automatic retry, alternate-address racing or 0-RTT.
func Dial(ctx context.Context, origin string, tlsConfig *tls.Config, quicConfig *quic.Config, config *ClientConfig) (*ClientConn, error) {
	client, err := dialHTTP(ctx, origin, tlsConfig, quicConfig, config, false)
	return client, httpError(err)
}

// DialEarly initializes HTTP/3 using remembered SETTINGS when a suitable session
// ticket exists. Otherwise it waits for the handshake. Ordinary request methods
// wait for TLS completion; MethodGet0RTT and MethodHead0RTT explicitly allow
// replayable early requests. Rejection is returned to the caller; use
// ClientConn.NextConnection to obtain a fresh session before explicitly retrying.
// Like Dial, cancellation of ctx after success does not close the client.
func DialEarly(ctx context.Context, origin string, tlsConfig *tls.Config, quicConfig *quic.Config, config *ClientConfig) (*ClientConn, error) {
	client, err := dialHTTP(ctx, origin, tlsConfig, quicConfig, config, true)
	return client, httpError(err)
}

func dialHTTP(ctx context.Context, origin string, tlsConfig *tls.Config, quicConfig *quic.Config, config *ClientConfig, early bool) (*ClientConn, error) {
	return dialHTTPWith(ctx, origin, tlsConfig, quicConfig, config, early, nil)
}

func dialHTTPWith(ctx context.Context, origin string, tlsConfig *tls.Config, quicConfig *quic.Config, config *ClientConfig, early bool, customDial func(context.Context, string, *tls.Config, *quic.Config) (*quic.Conn, error)) (*ClientConn, error) {
	if ctx == nil {
		return nil, fmt.Errorf("http3: nil dial context")
	}
	address, err := clientOrigin(origin)
	if err != nil {
		return nil, err
	}
	cfg, err := clientOptions(config)
	if err != nil {
		return nil, err
	}
	if ctx.Err() != nil {
		return nil, context.Cause(ctx)
	}
	tlsCopy := &tls.Config{}
	if tlsConfig != nil {
		tlsCopy = tlsConfig.Clone()
	}
	tlsCopy.NextProtos = []string{"h3"}
	if tlsCopy.ServerName == "" {
		tlsCopy.ServerName, _, err = net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
	}
	var cache *httpSessionCache
	if tlsCopy.ClientSessionCache != nil {
		cache = &httpSessionCache{base: tlsCopy.ClientSessionCache, scope: fmt.Sprintf("%s|h3|headers=%d|datagrams=%t", address, cfg.responseHeaderLimit(), cfg.EnableDatagrams) + additionalSettingsScope(cfg.AdditionalSettings)}
		tlsCopy.ClientSessionCache = cache
		cfg.peerSettings = cache.publishSettings
	}
	quicCopy := &quic.Config{}
	if quicConfig != nil {
		quicCopy = quicConfig.Clone()
	}
	quicCopy.EnableDatagrams = quicCopy.EnableDatagrams || cfg.EnableDatagrams
	dial := quic.DialAddr
	if early {
		dial = quic.DialAddrEarly
	}
	if customDial != nil {
		dial = customDial
	}
	conn, err := dial(ctx, address, tlsCopy, quicCopy)
	if err != nil {
		if conn != nil {
			conn.CloseWithError(0x100, "HTTP3 dialing failed")
		}
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		return nil, err
	}
	if conn == nil {
		return nil, fmt.Errorf("http3: dial returned a nil connection")
	}
	interrupted := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(interrupted); conn.CloseWithError(0x100, "HTTP3 dialing canceled") })
	var remembered *[]h3.Setting
	if early && cache != nil {
		if settings, ok := cache.rememberedSettings(); ok {
			remembered = &settings
		}
	}
	// A custom dialer can return before handshake completion even when HTTP
	// early data is disabled or no SETTINGS were remembered for this origin.
	// Wait before opening critical streams, and clear any rejected early epoch.
	if remembered == nil {
		if early || customDial != nil {
			_, err = conn.NextConnection(ctx)
		}
	} else {
		select {
		case <-conn.HandshakeComplete():
			// No HTTP streams exist yet, so any transport rejection can be reset
			// before creating the first ordinary HTTP session.
			_, err = conn.NextConnection(ctx)
			remembered = nil
		default:
		}
	}
	var client *ClientConn
	if err == nil {
		client, err = newClientConn(context.WithoutCancel(ctx), conn, origin, &cfg, remembered)
	}
	// Rejection during setup exposes no application handle or request. Discard
	// partial critical streams and initialize once more after explicit QUIC reset.
	if errors.Is(err, quic.Err0RTTRejected) && early {
		if _, err = conn.NextConnection(ctx); err == nil {
			client, err = NewClientConn(context.WithoutCancel(ctx), conn, origin, &cfg)
		}
	}
	if !stop() {
		<-interrupted
	}
	if ctx.Err() != nil {
		err = context.Cause(ctx)
	}
	if err != nil {
		if client != nil {
			client.Close()
		} else {
			conn.CloseWithError(0x100, "HTTP3 initialization failed")
		}
		return nil, err
	}
	return client, nil
}
