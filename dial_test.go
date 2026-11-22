package quic

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/getlantern/photon/internal/transport"
	"github.com/getlantern/photon/internal/wire"
	"io"
	"math/big"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"
)

func dialTLS(t testing.TB) (*tls.Config, *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(parsed)
	return &tls.Config{RootCAs: roots, NextProtos: []string{"test"}}, &tls.Config{NextProtos: []string{"test"}, Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
}
func TestPublicDialPair(t *testing.T) {
	for _, v := range []Version{Version1, Version2} {
		clientTLS, serverTLS := dialTLS(t)
		l, err := ListenAddr("127.0.0.1:0", serverTLS, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { l.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c, err := DialAddr(ctx, l.Addr().String(), clientTLS, &Config{Versions: []Version{v}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.server.Close() })
		if clientTLS.ServerName != "" {
			t.Fatal("mutated TLS config")
		}
		peer, err := l.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, conn := range []*Conn{c, peer} {
			select {
			case <-conn.HandshakeComplete():
			default:
				t.Fatal("established connection lacks handshake notification")
			}
			if state := conn.ConnectionState(); !state.TLS.HandshakeComplete || state.Used0RTT {
				t.Fatal("established state snapshot")
			}
		}
		s, err := c.OpenUniStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Write([]byte("client")); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		rx, err := peer.AcceptUniStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		rx.SetReadDeadline(time.Now().Add(time.Second))
		if b, err := io.ReadAll(rx); err != nil || string(b) != "client" {
			t.Fatal(string(b), err)
		}
		if err := c.CloseWithError(0, ""); err != nil {
			t.Fatal(err)
		}
		select {
		case <-c.server.Done():
		case <-ctx.Done():
			t.Fatal("client runtime did not stop", ctx.Err())
		}
	}
}
func TestDialFailureAndCancellation(t *testing.T) {
	clientTLS, serverTLS := dialTLS(t)
	l, err := ListenAddr("127.0.0.1:0", serverTLS, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	untrusted := clientTLS.Clone()
	untrusted.RootCAs = x509.NewCertPool()
	if c, err := DialAddr(ctx, l.Addr().String(), untrusted, nil); err == nil {
		c.server.Close()
		t.Fatal("accepted untrusted certificate")
	} else {
		var failure *TransportError
		var certificate *tls.CertificateVerificationError
		if !errors.As(err, &failure) || failure.Remote || !failure.ErrorCode.IsCryptoError() || !errors.As(err, &certificate) || !errors.Is(err, net.ErrClosed) {
			t.Fatal("TLS failure contract", err)
		}
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if _, err := DialAddr(canceled, l.Addr().String(), clientTLS, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	blackhole, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer blackhole.Close()
	short, stop := context.WithTimeout(ctx, 25*time.Millisecond)
	defer stop()
	if _, err := DialAddr(short, blackhole.LocalAddr().String(), clientTLS, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("dial timeout", err)
	}
}

func TestDialVersionRejection(t *testing.T) {
	clientTLS, serverTLS := dialTLS(t)
	l, err := ListenAddr("127.0.0.1:0", serverTLS, &Config{Versions: []Version{Version1}})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := DialAddr(ctx, l.Addr().String(), clientTLS, &Config{Versions: []Version{Version2}})
	if c != nil {
		c.server.Close()
		t.Fatal("unexpected connection")
	}
	var rejected *VersionNegotiationError
	if !errors.As(err, &rejected) || rejected.ChosenVersion != Version2 {
		t.Fatal("expected version rejection", err)
	}
	if !errors.Is(err, net.ErrClosed) || len(rejected.Ours) != 1 || rejected.Ours[0] != Version2 || len(rejected.Theirs) == 0 || rejected.Theirs[0] != Version1 {
		t.Fatalf("version rejection details: %+v", rejected)
	}
}

func TestDialVersionFallback(t *testing.T)           { testDialVersionFallback(t, false) }
func TestCompatibleDialVersionFallback(t *testing.T) { testDialVersionFallback(t, true) }
func testDialVersionFallback(t *testing.T, compatible bool) {
	for _, only := range []Version{Version1, Version2} {
		clientTLS, serverTLS := dialTLS(t)
		l, err := ListenAddr("127.0.0.1:0", serverTLS, &Config{Versions: []Version{only}})
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		original := Version1
		if only == Version1 {
			original = Version2
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		versions := []Version{original, only}
		if compatible {
			versions = []Version{only, original}
		}
		c, err := DialAddr(ctx, l.Addr().String(), clientTLS, &Config{InitialVersion: original, Versions: versions, EnableCompatibleVersionNegotiation: compatible})
		if err != nil {
			t.Fatal(err)
		}
		defer c.server.Close()
		if c.ConnectionState().Version != only {
			t.Fatal("wrong fallback")
		}
		if _, err := l.Accept(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDialRejectsForgedVersionDowngrade(t *testing.T) {
	testDialRejectsForgedVersionDowngrade(t, false, Version1)
}
func TestCompatibleDialRejectsForgedVersionDowngrade(t *testing.T) {
	for _, first := range []Version{Version1, Version2} {
		t.Run(fmt.Sprintf("%x", first), func(t *testing.T) { testDialRejectsForgedVersionDowngrade(t, true, first) })
	}
}
func testDialRejectsForgedVersionDowngrade(t *testing.T, compatible bool, first Version) {
	fallback := Version1
	if first == fallback {
		fallback = Version2
	}
	var serverVersion atomic.Uint32
	clientTLS, serverTLS := dialTLS(t)
	backend, err := ListenAddr("127.0.0.1:0", serverTLS, &Config{Versions: []Version{first, fallback}, EnableCompatibleVersionNegotiation: compatible})
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	proxy, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	destination := backend.Addr().(*net.UDPAddr).AddrPort()
	finished := make(chan error, 1)
	go func() {
		var client netip.AddrPort
		injected := false
		b := make([]byte, 65535)
		for {
			n, from, err := proxy.ReadFromUDPAddrPort(b)
			if err != nil {
				finished <- err
				return
			}
			if from == destination {
				if h, err := wire.ParseHeader(b[:n], 8); err == nil && h.Long && h.Type == wire.Initial {
					serverVersion.Store(uint32(h.Version))
				}
				if _, err := proxy.WriteToUDPAddrPort(b[:n], client); err != nil {
					finished <- err
					return
				}
				continue
			}
			client = from
			if !injected {
				header, err := wire.ParseHeader(b[:n], 8)
				if err != nil {
					finished <- err
					return
				}
				fake, err := wire.AppendVersionNegotiation(nil, 0, header.SCID, header.DCID, []wire.Version{fallback})
				if err != nil {
					finished <- err
					return
				}
				if _, err := proxy.WriteToUDPAddrPort(fake, from); err != nil {
					finished <- err
					return
				}
				injected = true
				continue
			}
			if _, err := proxy.WriteToUDPAddrPort(b[:n], destination); err != nil {
				finished <- err
				return
			}
		}
	}()
	defer func() { proxy.Close(); <-finished }()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, err := DialAddr(ctx, proxy.LocalAddr().String(), clientTLS, &Config{Versions: []Version{first, fallback}, EnableCompatibleVersionNegotiation: compatible})
	if c != nil {
		c.server.Close()
		t.Fatal("forged downgrade accepted")
	}
	var protocol *transport.TransportError
	if !errors.As(err, &protocol) || protocol.Code != 0x11 {
		t.Fatal("expected authenticated downgrade rejection", err)
	}
	var failure *TransportError
	if !errors.As(err, &failure) || failure.Remote || failure.ErrorCode != 0x11 || !errors.Is(err, net.ErrClosed) {
		t.Fatal("public downgrade failure", err)
	}
	want := fallback
	if compatible {
		want = first
	}
	if serverVersion.Load() != uint32(want) {
		t.Fatalf("server Initial version=%x want=%x", serverVersion.Load(), want)
	}
}
