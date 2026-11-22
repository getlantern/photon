package interop

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"testing"
	"time"

	engine "github.com/getlantern/photon/internal/transport"
	"github.com/getlantern/photon/internal/wire"
	upstream "github.com/quic-go/quic-go"
)

func tlsConfigs(t testing.TB) (*tls.Config, *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(9), DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	return &tls.Config{ServerName: "localhost", RootCAs: roots, NextProtos: []string{"independent-quic-test"}}, &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, NextProtos: []string{"independent-quic-test"}}
}

// drive is a test-only UDP adapter for one path and connection. The engine owns
// packetization, protection, TLS, ACK handling, and recovery deadlines. The
// 100ms read cap only checks context cancellation; it does not trigger probes.
func drive(ctx context.Context, c *engine.Conn, socket *net.UDPConn, peer *net.UDPAddr, ready ...func() bool) error {
	return driveProgress(ctx, c, socket, peer, nil, ready...)
}

func driveProgress(ctx context.Context, c *engine.Conn, socket *net.UDPConn, peer *net.UDPAddr, progress func() error, ready ...func() bool) error {
	buffer := make([]byte, 65535)
	for {
		if progress != nil {
			if err := progress(); err != nil {
				return err
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		for {
			b, err := c.NextDatagram(time.Now())
			if err != nil {
				return err
			}
			if b == nil {
				break
			}
			if _, err := socket.WriteToUDP(b, peer); err != nil {
				c.Close()
				return err
			}
		}
		if c.HandshakeConfirmed() && !c.HasOutstanding() && (len(ready) == 0 || ready[0]()) {
			return nil
		}
		deadline := time.Now().Add(100 * time.Millisecond)
		if recovery := c.RecoveryDeadline(); !recovery.IsZero() && recovery.Before(deadline) {
			deadline = recovery
		}
		socket.SetReadDeadline(deadline)
		n, from, err := socket.ReadFromUDP(buffer)
		if err != nil {
			if e, ok := err.(net.Error); ok && e.Timeout() {
				if err := c.HandleTimeout(time.Now()); err != nil {
					return err
				}
				continue
			}
			return err
		}
		if from.String() != peer.String() {
			continue
		}
		if err := c.HandleDatagram(buffer[:n], time.Now()); err != nil {
			return err
		}
	}
}

type peerResult struct {
	conn *upstream.Conn
	err  error
}

func TestUpstreamHandshake(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, mode := range []struct {
			name           string
			server, shared bool
		}{{"client", false, false}, {"server-default-client", true, false}, {"server-shared-client", true, true}} {
			server := mode.server
			t.Run(fmt.Sprintf("version-%x/%s", uint32(v), mode.name), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				clientTLS, serverTLS := tlsConfigs(t)
				qconfig := &upstream.Config{EnableDatagrams: true, Versions: []upstream.Version{upstream.Version(v)}, InitialStreamReceiveWindow: 4096, MaxStreamReceiveWindow: 4096, InitialConnectionReceiveWindow: 8192, MaxConnectionReceiveWindow: 8192}
				socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				defer socket.Close()
				result := make(chan peerResult, 1)
				config := engine.Config{Server: server, Version: v, LocalCID: []byte("localcid"), Parameters: wire.DefaultTransportParameters()}
				config.Parameters.MaxDatagramFrameSize = 65535
				config.Parameters.InitialMaxData = 8192
				config.Parameters.InitialMaxStreamDataBidiLocal = 4096
				config.Parameters.InitialMaxStreamDataBidiRemote = 4096
				config.Parameters.InitialMaxStreamsBidi = 4
				var peer *net.UDPAddr
				var first []byte
				if server {
					config.TLS = serverTLS
					if mode.shared {
						upSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
						if err != nil {
							t.Fatal(err)
						}
						defer upSocket.Close()
						tr := &upstream.Transport{Conn: upSocket, ConnectionIDLength: 8}
						defer tr.Close()
						go func() {
							conn, err := tr.Dial(ctx, socket.LocalAddr(), clientTLS, qconfig)
							result <- peerResult{conn, err}
						}()
					} else {
						go func() {
							conn, err := dialUpstreamAddr(t, ctx, socket.LocalAddr().String(), clientTLS, qconfig)
							result <- peerResult{conn, err}
						}()
					}
					socket.SetReadDeadline(time.Now().Add(4 * time.Second))
					first = make([]byte, 65535)
					n, addr, err := socket.ReadFromUDP(first)
					if err != nil {
						t.Fatal(err)
					}
					first = first[:n]
					peer = addr
					h, err := wire.ParseHeader(first, 0)
					if err != nil || h.Type != wire.Initial {
						t.Fatal("expected Initial", err)
					}
					config.PeerCID = append([]byte(nil), h.SCID...)
					config.OriginalDCID = append([]byte(nil), h.DCID...)
				} else {
					config.TLS = clientTLS
					config.OriginalDCID = make([]byte, 8)
					if _, err := rand.Read(config.OriginalDCID); err != nil {
						t.Fatal(err)
					}
					config.PeerCID = config.OriginalDCID
					listener, err := upstream.ListenAddr("127.0.0.1:0", serverTLS, qconfig)
					if err != nil {
						t.Fatal(err)
					}
					defer listener.Close()
					peer = listener.Addr().(*net.UDPAddr)
					go func() { conn, err := listener.Accept(ctx); result <- peerResult{conn, err} }()
				}
				c, err := engine.NewConn(config)
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				if err := c.Start(ctx); err != nil {
					t.Fatal(err)
				}
				if first != nil {
					if err := c.HandleDatagram(first, time.Now()); err != nil {
						t.Fatal(err)
					}
				}
				if err := drive(ctx, c, socket, peer); err != nil {
					t.Fatal("independent engine", err)
				}
				if !c.HandshakeComplete() || !c.HandshakeConfirmed() {
					t.Fatal("unconfirmed handshake")
				}
				if server && !mode.shared {
					if len(config.PeerCID) != 0 {
						t.Fatal("expected DialAddr zero-length client CID")
					}
					if err := c.RotatePeerID(); err == nil {
						t.Fatal("rotated zero-length peer CID")
					}
				} else {
					if err := drive(ctx, c, socket, peer, c.CanRotatePeerID); err != nil {
						t.Fatal("wait for spare peer CID", err)
					}
					if err := c.RotatePeerID(); err != nil {
						t.Fatal("rotate peer CID", err)
					}
					if err := drive(ctx, c, socket, peer); err != nil {
						t.Fatal("CID rotation", err)
					}
				}
				if !server && len(c.ConnectionState().VerifiedChains) == 0 {
					t.Fatal("independent client did not authenticate certificate")
				}
				select {
				case r := <-result:
					if r.err != nil {
						t.Fatal("upstream peer", r.err)
					}
					defer r.conn.CloseWithError(0, "interop test finished")
					state := r.conn.ConnectionState()
					if state.Version != upstream.Version(v) {
						t.Fatal("wrong negotiated version")
					}
					if server && len(state.TLS.VerifiedChains) == 0 {
						t.Fatal("upstream client did not authenticate certificate")
					}
					if err := exchangeDatagrams(ctx, c, socket, peer, r.conn); err != nil {
						t.Fatal("datagram exchange", err)
					}
					t.Run("path-validation", func(t *testing.T) {
						if !server && os.Getenv("QUIC_INTEROP_PATH_FAILURES") != "1" {
							t.Skip("quic-go v0.62.0 server ignores PATH_CHALLENGE on current path; set QUIC_INTEROP_PATH_FAILURES=1 to reproduce")
						}
						if err := c.ValidatePath(); err != nil {
							t.Fatal("start path validation", err)
						}
						if err := drive(ctx, c, socket, peer, func() bool { return c.PathValidationState() != engine.PathValidating }); err != nil {
							t.Fatal("path validation", err)
						}
						if c.PathValidationState() != engine.PathValidated {
							t.Fatal("upstream did not validate path")
						}
					})
					t.Run("stream", func(t *testing.T) {
						if err := exchangeStream(ctx, c, socket, peer, r.conn); err != nil {
							t.Fatal("stream exchange", err)
						}
						if err := c.InitiateKeyUpdate(); err != nil {
							t.Fatal("local key update", err)
						}
						if err := drive(ctx, c, socket, peer); err != nil {
							t.Fatal("local key update response", err)
						}
						if err := c.CloseWithError(42, "interop complete", time.Now()); err != nil {
							t.Fatal(err)
						}
						closing, err := c.NextDatagram(time.Now())
						if err != nil || len(closing) == 0 {
							t.Fatal("close packet", err)
						}
						if _, err := socket.WriteToUDP(closing, peer); err != nil {
							t.Fatal(err)
						}
						select {
						case <-r.conn.Context().Done():
							var appError *upstream.ApplicationError
							if !errors.As(context.Cause(r.conn.Context()), &appError) || appError.ErrorCode != 42 {
								t.Fatal("upstream close reason", context.Cause(r.conn.Context()))
							}
						case <-ctx.Done():
							t.Fatal("upstream did not receive close", ctx.Err())
						}

					})
				case <-ctx.Done():
					t.Fatal("upstream handshake did not complete", ctx.Err())
				}
			})
		}
	}
}
