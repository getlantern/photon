package handshake

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/packet"
	"github.com/getlantern/photon/internal/wire"
)

func configs(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &tls.Config{RootCAs: pool, ServerName: "localhost", NextProtos: []string{"quic-test"}}, &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, NextProtos: []string{"quic-test"}}
}

// Flights travel through our frame encoder, packet protection, header parser,
// authentication, frame parser, and reassembler before reaching the peer's TLS.
// This is an in-memory integration test, not an independent implementation test.
func transfer(t *testing.T, version wire.Version, from, to *TLS, initialWrite, initialRead *packet.Keys, numbers map[tls.QUICEncryptionLevel]uint64) int {
	t.Helper()
	flights := from.Drain()
	for _, flight := range flights {
		wk, rk := from.WriteKeys(flight.Level), to.ReadKeys(flight.Level)
		if flight.Level == tls.QUICEncryptionLevelInitial {
			wk, rk = initialWrite, initialRead
		}
		if wk == nil || rk == nil {
			t.Fatalf("missing %v keys", flight.Level)
		}
		// Reverse each flight's fragments, and duplicate each one. Each duplicate
		// gets a fresh packet number: CRYPTO retransmission is not nonce reuse.
		for end := len(flight.Data); end > 0; {
			start := max(0, end-137)
			for repeat := 0; repeat < 2; repeat++ {
				f := wire.CryptoFrame{Offset: flight.Offset + uint64(start), Data: flight.Data[start:end]}
				payload, err := f.Append(nil)
				if err != nil {
					t.Fatal(err)
				}
				pn := numbers[flight.Level]
				numbers[flight.Level]++
				typ := wire.Initial
				if flight.Level == tls.QUICEncryptionLevelHandshake {
					typ = wire.Handshake
				}
				var header []byte
				if flight.Level == tls.QUICEncryptionLevelApplication {
					header = []byte{0x43, byte(pn >> 24), byte(pn >> 16), byte(pn >> 8), byte(pn)}
				} else {
					header, err = wire.AppendLongHeader(nil, version, typ, []byte("peerCID0"), []byte("selfCID0"), nil, pn, 4, len(payload)+16)
					if err != nil {
						t.Fatal(err)
					}
				}
				p, err := wk.Seal(header, payload, pn)
				if err != nil {
					t.Fatal(err)
				}
				h, err := wire.ParseHeader(p, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, plain, gotPN, err := rk.Open(p[:h.PacketLen], h.PNOffset, pn)
				if err != nil || gotPN != pn {
					t.Fatal("packet authentication", err)
				}
				decoded, n, err := wire.ConsumeCryptoFrame(plain)
				if err != nil || n != len(plain) {
					t.Fatal("CRYPTO frame", err)
				}
				if err := to.HandleCrypto(flight.Level, decoded.Offset, decoded.Data); err != nil {
					t.Fatal(err)
				}
			}
			end = start
		}
	}
	return len(flights)
}

func TestProtectedTLSHandshake(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(map[wire.Version]string{wire.Version1: "v1", wire.Version2: "v2"}[v], func(t *testing.T) {
			cc, sc := configs(t)
			// Initial CIDs plus the server's original destination CID.
			cp, sp := append([]byte{0x0f, 8}, []byte("client00")...), append([]byte{0x0f, 8}, []byte("server00")...)
			sp = append(sp, 0, 8)
			sp = append(sp, []byte("original")...)
			client, err := New(false, v, cc, cp)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			server, err := New(true, v, sc, sp)
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err = client.Start(ctx); err != nil {
				t.Fatal(err)
			}
			if err = server.Start(ctx); err != nil {
				t.Fatal(err)
			}
			ck, sk, err := packet.InitialKeys(v, []byte("original"))
			if err != nil {
				t.Fatal(err)
			}
			cn, sn := make(map[tls.QUICEncryptionLevel]uint64), make(map[tls.QUICEncryptionLevel]uint64)
			for i := 0; i < 10; i++ {
				n := transfer(t, v, client, server, ck, ck, cn) + transfer(t, v, server, client, sk, sk, sn)
				if n == 0 {
					break
				}
			}
			if !client.Complete() || !server.Complete() {
				t.Fatal("handshake incomplete")
			}
			if p, ok := client.PeerParameters(); !ok || !bytes.Equal(p, sp) {
				t.Fatal("server parameters")
			}
			if p, ok := server.PeerParameters(); !ok || !bytes.Equal(p, cp) {
				t.Fatal("client parameters")
			}
			if len(client.ConnectionState().VerifiedChains) == 0 {
				t.Fatal("server certificate was not verified")
			}
			if cc.MinVersion != 0 || sc.SessionTicketsDisabled {
				t.Fatal("modified caller configuration")
			}
			for _, pair := range [][2]*TLS{{client, server}, {server, client}} {
				wk, rk := pair[0].WriteKeys(tls.QUICEncryptionLevelApplication), pair[1].ReadKeys(tls.QUICEncryptionLevelApplication)
				payload := []byte("authenticated application payload")
				p, err := wk.Seal([]byte{0x43, 0, 0, 0, 0}, payload, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, out, _, err := rk.Open(p, 1, 0)
				if err != nil || !bytes.Equal(out, payload) {
					t.Fatal("application keys disagree", err)
				}
			}
		})
	}
}

func TestRejectTLSConfiguration(t *testing.T) {
	for _, c := range []*tls.Config{nil, {}, {NextProtos: []string{"x"}, MinVersion: tls.VersionTLS12}, {NextProtos: []string{"x"}, MaxVersion: tls.VersionTLS12}} {
		if h, err := New(false, wire.Version1, c, []byte{15, 0}); err == nil {
			h.Close()
			t.Fatal("invalid configuration accepted")
		}
	}
}

func TestTLSRejectsMalformedPeerParameters(t *testing.T) {
	for _, serverSends := range []bool{false, true} {
		t.Run(map[bool]string{false: "client", true: "server"}[serverSends], func(t *testing.T) {
			cc, sc := configs(t)
			client, err := New(false, wire.Version1, cc, []byte{15, 0})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			server, err := New(true, wire.Version1, sc, []byte{15, 0, 0, 0})
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			sender, receiver := client, server
			if serverSends {
				sender, receiver = server, client
			}
			// Bypass local validation to model a malicious independent peer. TLS still
			// signs/authenticates this extension, so transport validation must reject it.
			sender.conn.SetTransportParameters([]byte{15, 0, 15, 0})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := client.Start(ctx); err != nil {
				t.Fatal(err)
			}
			if err := server.Start(ctx); err != nil {
				t.Fatal(err)
			}
			var rejected error
			for step := 0; step < 8 && rejected == nil; step++ {
				count := 0
				for _, pair := range [][2]*TLS{{client, server}, {server, client}} {
					for _, flight := range pair[0].Drain() {
						count++
						if err := pair[1].HandleCrypto(flight.Level, flight.Offset, flight.Data); err != nil {
							rejected = err
							break
						}
					}
					if rejected != nil {
						break
					}
				}
				if count == 0 {
					break
				}
			}
			if !errors.Is(rejected, wire.ErrTransportParameter) {
				t.Fatalf("expected rejected peer parameters, got %v", rejected)
			}
			if receiver.Complete() {
				t.Fatal("completed TLS with malformed peer parameters")
			}
			if err := receiver.HandleCrypto(tls.QUICEncryptionLevelInitial, 0, nil); !errors.Is(err, wire.ErrTransportParameter) {
				t.Fatal("failure not sticky", err)
			}
		})
	}
}
