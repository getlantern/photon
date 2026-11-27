package transport

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

func testTLS(t testing.TB) (*tls.Config, *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(2), DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(parsed)
	return &tls.Config{ServerName: "localhost", RootCAs: pool, NextProtos: []string{"quic-engine"}}, &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, NextProtos: []string{"quic-engine"}}
}
func testPair(t testing.TB, v wire.Version, parameters ...wire.TransportParameters) (*Conn, *Conn) {
	t.Helper()
	clientTLS, serverTLS := testTLS(t)
	config := Config{Version: v, TLS: clientTLS, LocalCID: []byte("client00"), PeerCID: []byte("original"), OriginalDCID: []byte("original"), Parameters: wire.DefaultTransportParameters()}
	if len(parameters) > 0 {
		config.Parameters = parameters[0]
	}
	client, err := NewConn(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	config.Server = true
	config.TLS = serverTLS
	config.LocalCID = []byte("server00")
	config.PeerCID = []byte("client00")
	server, err := NewConn(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return client, server
}
func drain(t testing.TB, c *Conn, now time.Time) [][]byte {
	t.Helper()
	var out [][]byte
	// Large negotiated stream windows can emit more than 100 small packets.
	for i := 0; i < 4096; i++ {
		before := c.sentTotals
		b, err := c.NextDatagram(now)
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			if c.sentTotals != before {
				t.Fatal("empty emission changed send accounting")
			}
			return out
		}
		if c.sentTotals.packets != before.packets+1 || c.sentTotals.bytes != before.bytes+uint64(len(b)) {
			t.Fatal("emitted packet accounting mismatch", before, c.sentTotals, len(b))
		}
		out = append(out, b)
	}
	t.Fatal("output did not quiesce")
	return nil
}
func drivePair(t testing.TB, client, server *Conn, reverse, loss bool) {
	t.Helper()
	now := time.Unix(100, 0)
	dropped := map[wire.PacketType]bool{}
	for step := 0; step < 100; step++ {
		transmissions := 0
		for _, pair := range [][2]*Conn{{client, server}, {server, client}} {
			flights := drain(t, pair[0], now)
			if reverse {
				for i, j := 0, len(flights)-1; i < j; i, j = i+1, j-1 {
					flights[i], flights[j] = flights[j], flights[i]
				}
			}
			for _, b := range flights {
				h, err := pair[1].parseHeader(b)
				if err != nil {
					t.Fatal(err)
				}
				packetLimit := 1200
				if h.Type == wire.Initial {
					packetLimit = int(pair[0].config.InitialPacketSize)
					if params := pair[0].peerParams; params != nil {
						packetLimit = min(packetLimit, int(params.MaxUDPPayloadSize))
					}
					if len(b) != packetLimit {
						t.Fatal("Initial not padded to configured limit")
					}
				}
				if len(b) > packetLimit {
					t.Fatal("packet exceeds conservative path MTU")
				}
				transmissions++
				if loss && !dropped[h.Type] {
					dropped[h.Type] = true
					continue
				}
				if err := pair[1].HandleDatagram(b, now); err != nil {
					t.Fatalf("step %d packet %v server sender=%v: %v", step, h.Type, pair[0].config.Server, err)
				}
				if reverse {
					if err := pair[1].HandleDatagram(b, now); err != nil {
						t.Fatal("duplicate", err)
					}
				}
			}
		}
		if client.confirmed && server.confirmed && transmissions == 0 && client.queuedBytes == 0 && server.queuedBytes == 0 {
			return
		}
		if transmissions == 0 {
			// Advance the simulated clock to the real recovery deadline.
			var deadline time.Time
			for _, c := range []*Conn{client, server} {
				at := c.RecoveryDeadline()
				if !at.IsZero() && (deadline.IsZero() || at.Before(deadline)) {
					deadline = at
				}
			}
			if deadline.After(now) {
				now = deadline
			}
			if err := client.HandleTimeout(now); err != nil {
				t.Fatal(err)
			}
			if err := server.HandleTimeout(now); err != nil {
				t.Fatal(err)
			}
		}
		now = now.Add(10 * time.Millisecond)
	}
	t.Fatalf("handshake failed to converge: complete %v/%v confirmed %v/%v", client.complete, server.complete, client.confirmed, server.confirmed)
}
func TestEngineHandshake(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, mode := range []struct {
			name          string
			reverse, loss bool
		}{{"ordered", false, false}, {"reorder-duplicate", true, false}, {"loss", false, true}, {"loss-reorder", true, true}} {
			t.Run(vName(v)+"/"+mode.name, func(t *testing.T) {
				client, server := testPair(t, v)
				drivePair(t, client, server, mode.reverse, mode.loss)
				if len(client.ConnectionState().VerifiedChains) == 0 {
					t.Fatal("server identity not verified")
				}
				for _, c := range []*Conn{client, server} {
					if !c.complete || !c.confirmed || c.peerParams == nil {
						t.Fatal("handshake state")
					}
					if c.initialRead != nil || c.initialWrite != nil || c.readKeys(handshakeSpace) != nil || c.writeKeys(handshakeSpace) != nil {
						t.Fatal("old keys retained")
					}
					if c.queuedBytes != 0 {
						t.Fatalf("unretired handshake data: %d", c.queuedBytes)
					}
					if len(c.deferred) != 0 {
						t.Fatal("deferred packets leaked")
					}
				}
				if !server.budget.Validated() {
					t.Fatal("server path not validated")
				}
			})
		}
	}
}
func vName(v wire.Version) string {
	if v == wire.Version2 {
		return "v2"
	}
	return "v1"
}

// makePeerPacket builds an authenticated packet with arbitrary frames, bypassing
// the sender engine for negative protocol tests. Every call uses a distinct
// packet number for a given connection's key set.
func makePeerPacket(t testing.TB, sender *Conn, space int, pn uint64, frames ...wire.Frame) []byte {
	t.Helper()
	var payload []byte
	for _, f := range frames {
		var err error
		payload, err = f.Append(payload)
		if err != nil {
			t.Fatal(err)
		}
	}
	header, err := sender.makeHeader(space, pn, len(payload)+16)
	if err != nil {
		t.Fatal(err)
	}
	if space == initialSpace {
		payload = append(payload, make([]byte, 1200-len(header)-len(payload)-16)...)
		header, err = sender.makeHeader(space, pn, len(payload)+16)
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(payload) < 1 {
		payload = append(payload, 0)
	}
	b, err := sender.writeKeys(space).Seal(header, payload, pn)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestEngineInputDiscard(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	now := time.Unix(100, 0)
	flights := drain(t, client, now)
	if len(flights) == 0 {
		t.Fatal("missing ClientHello")
	}
	input := flights[0]
	for _, bad := range [][]byte{nil, {1, 2, 3}, input[:100], bytes.Clone(input)} {
		if len(bad) == len(input) {
			bad[len(bad)-1] ^= 1
		}
		if err := server.HandleDatagram(bad, now); err != nil {
			t.Fatal("garbage caused protocol failure", err)
		}
	}
	if server.spaces[initialSpace].received.Expected() != 0 {
		t.Fatal("unauthenticated packet entered receive history")
	}
	if server.peerParams != nil {
		t.Fatal("unauthenticated parameters")
	}
}
func TestEngineRejectsUnsentACK(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	b := makePeerPacket(t, client, initialSpace, 10, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}})
	err := server.HandleDatagram(b, time.Now())
	var te *TransportError
	if !errors.As(err, &te) || te.Code != 0xa {
		t.Fatal("ACK of unsent packet", err)
	}
	if next := server.QueueProbe(); next != err {
		t.Fatal("failure not sticky")
	}
}
func TestEngineRejectsFrameContext(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	b := makePeerPacket(t, client, initialSpace, 10, wire.MaxDataFrame{Maximum: 100})
	err := server.HandleDatagram(b, time.Now())
	var te *TransportError
	if !errors.As(err, &te) || te.Code != 0xa || te.FrameType != 0x10 {
		t.Fatal(err)
	}
}
func TestEngineACKDoesNotCauseACKLoop(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	// The actual exchange already acknowledges HANDSHAKE_DONE. Its pure ACK
	// must not cause another pure ACK in either direction.
	if out := drain(t, client, time.Now()); len(out) != 0 {
		t.Fatal("client ACK loop")
	}
	if out := drain(t, server, time.Now()); len(out) != 0 {
		t.Fatal("server ACK loop")
	}
}
func TestEngineCoalesced(t *testing.T) {
	client, server := testPair(t, wire.Version2)
	now := time.Now()
	for _, b := range drain(t, client, now) {
		if err := server.HandleDatagram(b, now); err != nil {
			t.Fatal(err)
		}
	}
	flights := drain(t, server, now)
	if len(flights) < 2 {
		t.Fatal("expected server flight")
	}
	var datagram []byte
	for _, b := range flights {
		datagram = append(datagram, b...)
	}
	if err := client.HandleDatagram(datagram, now); err != nil {
		t.Fatal(err)
	}
	if client.receivedTotals != (receiveAccounting{packets: uint64(len(flights)), bytes: uint64(len(datagram))}) {
		t.Fatal("coalesced receive totals", client.receivedTotals)
	}
	drivePair(t, client, server, false, false)
}
func TestEngineCoalescedCorruptionDoesNotHideLaterPackets(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	now := time.Now()
	flights := drain(t, client, now)
	// Corrupt an extra copy of the first packet, then include the original
	// flights. Parsing must continue after the first authentication failure.
	corrupt := bytes.Clone(flights[0])
	corrupt[len(corrupt)-1] ^= 1
	datagram := corrupt
	for _, b := range flights {
		datagram = append(datagram, b...)
	}
	if err := server.HandleDatagram(datagram, now); err != nil {
		t.Fatal(err)
	}
	if server.peerParams == nil {
		t.Fatal("ignored later valid Initial")
	}
	if server.receivedTotals != (receiveAccounting{packets: uint64(len(flights) + 1), bytes: uint64(len(datagram))}) {
		t.Fatal("corrupt coalesced receive totals", server.receivedTotals)
	}
	drivePair(t, client, server, false, false)
}
func TestEngineAntiAmplification(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	now := time.Now()
	if b, err := server.NextDatagram(now); err != nil || b != nil {
		t.Fatal("server sent before receiving")
	}
	// Deliver the full ClientHello, then clamp credit to exercise gating with
	// already queued encrypted flights. Credit is a path property, not TLS state.
	for _, b := range drain(t, client, now) {
		if err := server.HandleDatagram(b, now); err != nil {
			t.Fatal(err)
		}
	}
	server.budget.credit = 0
	next := server.spaces[initialSpace].nextPN
	if b, err := server.NextDatagram(now); err != nil || b != nil {
		t.Fatal("exceeded budget", err)
	}
	if server.spaces[initialSpace].nextPN != next {
		t.Fatal("blocked send consumed packet number")
	}
	server.budget.Received(1200)
	total := 0
	for _, b := range drain(t, server, now) {
		total += len(b)
	}
	if total > 3600 {
		t.Fatal("amplification limit", total)
	}
}
func TestEngineDeferredBound(t *testing.T) {
	_, server := testPair(t, wire.Version1)
	// Well-formed Handshake headers, no keys available; ciphertext need not be
	// valid to exercise the unauthenticated buffering limit.
	for i := 0; i < 100; i++ {
		h, err := wire.AppendLongHeader(nil, wire.Version1, wire.Handshake, server.config.LocalCID, server.peerCID, nil, uint64(i), 4, 1100)
		if err != nil {
			t.Fatal(err)
		}
		b := append(h, make([]byte, 1100)...)
		if err := server.HandleDatagram(b, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if len(server.deferred) > 64 || server.deferredBytes > maxDeferredBytes {
		t.Fatal("unbounded undecryptable packet buffering")
	}
}
func TestEnginePacketNumberExhaustion(t *testing.T) {
	client, _ := testPair(t, wire.Version1)
	client.spaces[initialSpace].nextPN = wire.MaxVarint + 1
	if _, err := client.NextDatagram(time.Now()); !errors.Is(err, wire.ErrPacketNumber) {
		t.Fatal(err)
	}
}
func TestEngineRejectsCIDMismatch(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	// The client TLS extension authenticates its configured CID, but every
	// packet carries a different SCID. Server routing observations must win.
	server.peerCID = []byte("attacker")
	server.initialPeerCID = []byte("attacker")
	server.config.PeerCID = clone(server.peerCID)
	client.config.LocalCID = []byte("attacker")
	var got error
	for _, b := range drain(t, client, time.Now()) {
		got = server.HandleDatagram(b, time.Now())
		if got != nil {
			break
		}
	}
	if !errors.Is(got, wire.ErrTransportParameter) {
		t.Fatal("CID mismatch accepted", got)
	}
}
func TestEngineInvalidConfiguration(t *testing.T) {
	cc, _ := testTLS(t)
	base := Config{Version: wire.Version1, TLS: cc, LocalCID: []byte("client00"), PeerCID: []byte("original"), OriginalDCID: []byte("original"), Parameters: wire.DefaultTransportParameters()}
	for _, mutate := range []func(*Config){
		func(c *Config) { c.Version = 100 }, func(c *Config) { c.OriginalDCID = []byte{1} }, func(c *Config) { c.PeerCID = []byte("different") },
		func(c *Config) { c.Parameters.InitialMaxStreamsBidi = 1<<60 + 1 },
	} {
		bad := base
		mutate(&bad)
		if c, err := NewConn(bad); err == nil {
			c.Close()
			t.Fatal("accepted unsupported configuration")
		}
	}
}

func TestEngineDoesNotReusePacketNumbersOnProbe(t *testing.T) {
	client, _ := testPair(t, wire.Version1)
	now := time.Now()
	keys, _, _ := packet.InitialKeys(wire.Version1, client.config.OriginalDCID)
	first := drain(t, client, now)
	if err := client.QueueProbe(); err != nil {
		t.Fatal(err)
	}
	second := drain(t, client, now)
	seen := make(map[uint64]bool)
	for _, b := range append(first, second...) {
		h, _ := wire.ParseHeader(b, 8)
		_, _, pn, err := keys.Open(b, h.PNOffset, 0)
		if err != nil {
			t.Fatal(err)
		}
		if seen[pn] {
			t.Fatal("nonce reuse")
		}
		seen[pn] = true
	}
	if len(second) == 0 {
		t.Fatal("probe didn't retransmit")
	}
}

func TestEngineAutomaticLossRecovery(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, timed := range []bool{false, true} {
			name := vName(version) + "/packet-threshold"
			if timed {
				name = vName(version) + "/time-threshold"
			}
			t.Run(name, func(t *testing.T) {
				client, server := testPair(t, version)
				now := time.Unix(100, 0)
				// Ensure enough later transmissions to exercise packet threshold 3.
				for i := 0; i < 3; i++ {
					if err := client.enqueue(initialSpace, wire.PingFrame{}, 1); err != nil {
						t.Fatal(err)
					}
				}
				original := client.spaces[initialSpace].queue[0]
				flights := drain(t, client, now)
				index := len(flights) - 1
				if timed {
					index = 1
				}
				// Lose the first CRYPTO packet. Only a later packet reaches the peer.
				if err := server.HandleDatagram(flights[index], now.Add(5*time.Millisecond)); err != nil {
					t.Fatal(err)
				}
				for _, b := range drain(t, server, now.Add(5*time.Millisecond)) {
					if err := client.HandleDatagram(b, now.Add(10*time.Millisecond)); err != nil {
						t.Fatal(err)
					}
				}
				if timed {
					if original.queued {
						t.Fatal("premature loss")
					}
					deadline := client.LossDeadline()
					if !deadline.Equal(now.Add(11250000 * time.Nanosecond)) {
						t.Fatal("wrong deadline", deadline)
					}
					if err := client.DetectLoss(deadline.Add(-time.Nanosecond)); err != nil {
						t.Fatal(err)
					}
					if original.queued {
						t.Fatal("timer fired early")
					}
					if err := client.DetectLoss(deadline); err != nil {
						t.Fatal(err)
					}
				}
				if !original.queued || original.acked {
					t.Fatal("lost CRYPTO not automatically requeued")
				}
				if !client.RTTStats().Sampled {
					t.Fatal("missing RTT sample")
				}
				// A late ACK of the lost original retires the shared item, preventing
				// queued retransmissions from resending already delivered CRYPTO data.
				if err := client.handleAck(initialSpace, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: 0}}}, now.Add(15*time.Millisecond)); err != nil {
					t.Fatal(err)
				}
				if !original.acked {
					t.Fatal("late ACK did not retire data")
				}
			})
		}
	}
}

func TestEngineACKRangeValidationDoesNotIteratePacketNumbers(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	// A range spanning 2^62 packet numbers must fail quickly using largestSent,
	// never loop over each acknowledged integer.
	b := makePeerPacket(t, client, initialSpace, 10, wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: wire.MaxVarint}}})
	if err := server.HandleDatagram(b, time.Now()); err == nil {
		t.Fatal("accepted impossible ACK range")
	}
}

func TestEngineRejectsVersionMismatch(t *testing.T) {
	_, server := testPair(t, wire.Version1)
	other, control := testPair(t, wire.Version2)
	now := time.Now()
	flights := drain(t, other, now)
	if len(flights) == 0 {
		t.Fatal("missing ClientHello")
	}
	for _, b := range flights {
		// Rewriting only the version field would turn the type bits into Retry.
		h, err := server.parseHeader(b)
		if err != nil || h.Type != wire.Initial || h.Version != wire.Version2 || !server.acceptCID(h) {
			t.Fatal("fixture is not a routable Initial of the other version", h, err)
		}
		if err := server.HandleDatagram(b, now); err != nil {
			t.Fatal(err)
		}
		if err := control.HandleDatagram(b, now); err != nil {
			t.Fatal(err)
		}
	}
	if control.authenticatedPackets == 0 || control.peerParams == nil {
		t.Fatal("fixture is not a valid Initial for its own version")
	}
	if server.authenticatedPackets != 0 || server.peerParams != nil || len(server.deferred) != 0 || server.deferredBytes != 0 {
		t.Fatal("wrong version reached receive, TLS, or deferred state")
	}
	if out := drain(t, server, now); len(out) != 0 || server.spaces[initialSpace].ackPending {
		t.Fatal("wrong version produced output")
	}
}

func TestEngineLostClientFinished(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	now := time.Unix(100, 0)
	dropped := false
	for step := 0; step < 100; step++ {
		count := 0
		for _, pair := range [][2]*Conn{{client, server}, {server, client}} {
			dropHandshakeFlight := pair[0] == client && !dropped
			for _, b := range drain(t, pair[0], now) {
				h, err := pair[1].parseHeader(b)
				if err != nil {
					t.Fatal(err)
				}
				count++
				if dropHandshakeFlight && h.Type == wire.Handshake {
					// The first client Handshake packet can be a pure ACK. Drop all
					// Handshake output from this flight, including Finished, instead.
					dropped = true
					continue
				}
				if err := pair[1].HandleDatagram(b, now); err != nil {
					t.Fatal(err)
				}
			}
		}
		if client.confirmed && server.confirmed {
			if !dropped {
				t.Fatal("did not drop client Handshake")
			}
			return
		}
		if count == 0 {
			client.QueueProbe()
			server.QueueProbe()
		}
		now = now.Add(time.Millisecond)
	}
	t.Fatal("lost client Finished not recovered")
}

func TestEngineProbeRetirement(t *testing.T) {
	client, _ := testPair(t, wire.Version1)
	now := time.Now()
	drain(t, client, now)
	space := &client.spaces[initialSpace]
	if err := client.QueueProbe(); err != nil {
		t.Fatal(err)
	}
	// A late ACK for an earlier transmission must cancel queued retransmission
	// of the same CRYPTO bytes, without changing their stream offsets.
	ack := wire.AckFrame{Ranges: []wire.AckRange{{Smallest: 0, Largest: space.largestSent}}}
	if err := client.handleAck(initialSpace, ack, now); err != nil {
		t.Fatal(err)
	}
	if out := drain(t, client, now); len(out) != 0 {
		t.Fatal("sent acknowledged probe")
	}
	if client.queuedBytes != 0 || len(space.sent) != 0 {
		t.Fatal("acknowledged CRYPTO retained")
	}
}

func TestEngineCloseAndStartGuards(t *testing.T) {
	client, _ := testPair(t, wire.Version1)
	if err := client.Start(context.Background()); err == nil {
		t.Fatal("started twice")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.NextDatagram(time.Now()); err == nil {
		t.Fatal("send after close")
	}
	if err := client.HandleDatagram(nil, time.Now()); err == nil {
		t.Fatal("receive after close")
	}
	if err := client.Close(); err != nil {
		t.Fatal("non-idempotent close", err)
	}
}

func FuzzEngineInitialInput(f *testing.F) {
	f.Add([]byte{0xc0, 0, 0, 0, 1}, false)
	f.Add([]byte{1}, true)
	// Certificate generation and configuration are shared, connection state isn't.
	_, serverTLS := testTLS(f)
	config := Config{Server: true, Version: wire.Version1, TLS: serverTLS, LocalCID: []byte("server00"), PeerCID: []byte("client00"), OriginalDCID: []byte("original"), Parameters: wire.DefaultTransportParameters()}
	f.Fuzz(func(t *testing.T, input []byte, authenticated bool) {
		if len(input) > 4096 {
			return
		}
		c, err := NewConn(config)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		if err := c.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		data := input
		if authenticated {
			// Exercise frame/TLS dispatch past AEAD authentication with arbitrary
			// plaintext while preserving the Initial datagram size requirement.
			plain := clone(input)
			if len(plain) < 1150 {
				plain = append(plain, make([]byte, 1150-len(plain))...)
			}
			h, err := wire.AppendLongHeader(nil, config.Version, wire.Initial, config.OriginalDCID, config.PeerCID, nil, 0, 4, len(plain)+16)
			if err != nil {
				t.Fatal(err)
			}
			client, _, err := packet.InitialKeys(config.Version, config.OriginalDCID)
			if err != nil {
				t.Fatal(err)
			}
			data, err = client.Seal(h, plain, 0)
			if err != nil {
				t.Fatal(err)
			}
			// Smallest long headers can put 1150-byte payloads below 1200. Append
			// datagram padding; the protected packet's own length remains unchanged.
			if len(data) < 1200 {
				data = append(data, make([]byte, 1200-len(data))...)
			}
		}
		before := bytes.Clone(data)
		_ = c.HandleDatagram(data, time.Unix(100, 0))
		if !bytes.Equal(before, data) {
			t.Fatal("input mutated")
		}
		if c.deferredBytes > maxDeferredBytes || c.queuedBytes > maxSendBytes {
			t.Fatal("unbounded buffers")
		}
	})
}
