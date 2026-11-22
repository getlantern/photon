package interop

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	"github.com/getlantern/photon/internal/wire"
)

// The resumed relay holds server input until a real early request leaves, then
// holds client Handshake packets until the peer emits a 1-RTT packet. Client
// 1-RTT output is discarded until the test finishes reading the early response,
// so reliable retransmission cannot conceal failure to process early traffic.
type compatibleEarlyRelay struct {
	front, back             *net.UDPConn
	early, response, done   chan struct{}
	application             chan struct{}
	earlyOnce, responseOnce sync.Once
	readers                 sync.WaitGroup
	earlyVersion            atomic.Uint32
	serverVersion           atomic.Uint32
	versionNegotiations     atomic.Int32
}

func newCompatibleEarlyRelay(t *testing.T, remote *net.UDPAddr) *compatibleEarlyRelay {
	t.Helper()
	front, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	back, err := net.DialUDP("udp4", nil, remote)
	if err != nil {
		front.Close()
		t.Fatal(err)
	}
	r := &compatibleEarlyRelay{front: front, back: back, early: make(chan struct{}), response: make(chan struct{}), application: make(chan struct{}), done: make(chan struct{})}
	clientAddress := make(chan *net.UDPAddr, 1)
	r.readers.Add(2)
	go func() {
		defer r.readers.Done()
		buf := make([]byte, 65535)
		first := true
		for {
			n, client, err := front.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if first {
				clientAddress <- client
				first = false
			}
			// Our sender emits one packet per datagram; short-header packets
			// therefore contain only application-space traffic.
			if buf[0]&0x80 == 0 {
				select {
				case <-r.application:
				default:
					continue
				}
			}
			handshake, early := false, false
			for rest := buf[:n]; len(rest) > 0; {
				h, err := wire.ParseHeader(rest, 8)
				if err != nil || h.PacketLen <= 0 {
					break
				}
				if h.Type == wire.Handshake {
					handshake = true
				}
				if h.Type == wire.ZeroRTT {
					early = true
					r.earlyVersion.Store(uint32(h.Version))
				}
				rest = rest[h.PacketLen:]
			}
			if handshake {
				select {
				case <-r.response:
				case <-r.done:
					return
				}
			}
			if _, err := back.Write(buf[:n]); err != nil {
				return
			}
			if early {
				r.earlyOnce.Do(func() { close(r.early) })
			}
		}
	}()
	go func() {
		defer r.readers.Done()
		var client *net.UDPAddr
		select {
		case client = <-clientAddress:
		case <-r.done:
			return
		}
		buf := make([]byte, 65535)
		for {
			n, err := back.Read(buf)
			if err != nil {
				return
			}
			select {
			case <-r.early:
			case <-r.done:
				return
			}
			for rest := buf[:n]; len(rest) > 0; {
				h, err := wire.ParseHeader(rest, 8)
				if err != nil || h.PacketLen <= 0 {
					break
				}
				if h.Long {
					if h.Version == 0 {
						r.versionNegotiations.Add(1)
					} else {
						r.serverVersion.Store(uint32(h.Version))
					}
				}
				if h.Type == wire.OneRTT {
					r.responseOnce.Do(func() { close(r.response) })
				}
				rest = rest[h.PacketLen:]
			}
			if _, err := front.WriteToUDP(buf[:n], client); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { close(r.done); front.Close(); back.Close(); r.readers.Wait() })
	return r
}

func TestPicoquicEarlyClientControl(t *testing.T)      { testPicoquicEarlyClient(t, false, true) }
func TestPicoquicEarlyCompatibleRecovery(t *testing.T) { testPicoquicEarlyClient(t, true, false) }
func TestPicoquicEarlyCompatibleAcceptance(t *testing.T) {
	if os.Getenv("QUIC_INTEROP_KNOWN_FAILURES") != "1" {
		t.Skip("pinned peer rejects ticket during compatible switching; see KNOWN_FAILURES.md")
	}
	testPicoquicEarlyClient(t, true, true)
}
func testPicoquicEarlyClient(t *testing.T, compatible, requireAcceptance bool) {
	binary := os.Getenv("PICOQUIC_DEMO")
	if binary == "" {
		t.Skip("set PICOQUIC_DEMO to the pinned peer")
	}
	ct, st := tlsConfigs(t)
	ct.NextProtos = []string{"hq-interop"}
	cache := &observedSessionCache{ClientSessionCache: tls.NewLRUClientSessionCache(4), stored: make(chan bool, 8)}
	ct.ClientSessionCache = cache
	dir := t.TempDir()
	key, err := x509.MarshalPKCS8PrivateKey(st.Certificates[0].PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{"cert.pem": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: st.Certificates[0].Certificate[0]}), "key.pem": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}), "early.txt": bytes.Repeat([]byte("early compatible response\n"), 8)}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	address := socket.LocalAddr().(*net.UDPAddr)
	socket.Close()
	log, err := os.Create(filepath.Join(dir, "peer.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "-a", "hq-interop", "-p", strconv.Itoa(address.Port), "-c", filepath.Join(dir, "cert.pem"), "-k", filepath.Join(dir, "key.pem"), "-w", dir, "-m", "1200")
	cmd.Dir = dir
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	defer func() {
		cmd.Process.Kill()
		cmd.Wait()
		log.Close()
		if t.Failed() {
			data, _ := os.ReadFile(filepath.Join(dir, "peer.log"))
			t.Logf("picoquic: %s", data)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	seed, err := quic.DialAddrEarly(ctx, address.String(), ct, &quic.Config{Versions: []quic.Version{quic.Version1}})
	if err != nil {
		t.Fatal(err)
	}
	defer seed.CloseWithError(0, "")
	waitEarlyTicket(t, ctx, cache)
	relay := newCompatibleEarlyRelay(t, address)

	selected := quic.Version1
	config := &quic.Config{Versions: []quic.Version{quic.Version1}}
	if compatible {
		selected = quic.Version2
		config = &quic.Config{Versions: []quic.Version{quic.Version2, quic.Version1}, InitialVersion: quic.Version1, EnableCompatibleVersionNegotiation: true}
	}
	c, err := quic.DialAddrEarly(ctx, relay.front.LocalAddr().String(), ct, config)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseWithError(0, "")
	select {
	case <-c.HandshakeComplete():
		t.Fatal("not early")
	default:
	}
	stream, err := c.OpenStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stream.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := stream.Write([]byte("GET /early.txt\r\n")); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(stream)
	if !requireAcceptance {
		if !errors.Is(err, quic.Err0RTTRejected) {
			t.Fatalf("expected pinned-peer rejection, got %v", err)
		}
		relay.responseOnce.Do(func() { close(relay.response) })
		close(relay.application)
		recovered, recoverErr := c.NextConnection(ctx)
		if recoverErr != nil || recovered != c {
			t.Fatal("recovery", recoverErr)
		}
		fresh, openErr := c.OpenStreamSync(ctx)
		if openErr != nil {
			t.Fatal(openErr)
		}
		if fresh.StreamID() != stream.StreamID() {
			t.Fatal("stream ID not reset")
		}
		stream.CancelRead(42)
		stream.CancelWrite(43)
		fresh.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := fresh.Write([]byte("GET /early.txt\r\n")); err != nil {
			t.Fatal(err)
		}
		if err := fresh.Close(); err != nil {
			t.Fatal(err)
		}
		got, err = io.ReadAll(fresh)
	} else {
		close(relay.application)
	}
	if err != nil || !bytes.Equal(got, files["early.txt"]) {
		t.Fatalf("response length=%d: %v", len(got), err)
	}
	waitEarlyHandshake(t, ctx, c.HandshakeComplete())
	state := c.ConnectionState()
	if state.Used0RTT != requireAcceptance || state.TLS.DidResume != requireAcceptance || state.Version != selected {
		t.Fatalf("resumed=%v early=%v version=%x", state.TLS.DidResume, state.Used0RTT, state.Version)
	}
	if relay.earlyVersion.Load() != uint32(quic.Version1) || relay.serverVersion.Load() != uint32(selected) || relay.versionNegotiations.Load() != 0 {
		t.Fatal("wrong wire versions or incompatible negotiation")
	}
}
