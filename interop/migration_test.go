package interop

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	independent "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/qlogwriter"
)

func TestIndependentClientMigrationUpstreamServer(t *testing.T) {
	for _, version := range []independent.Version{independent.Version1, independent.Version2} {
		for _, loss := range []string{"clean", "request", "response", "both"} {
			t.Run(fmt.Sprintf("%x/%s", uint32(version), loss), func(t *testing.T) {
				t.Parallel()
				runIndependentMigration(t, version, loss, false)
			})
		}
	}
}

func TestKeepAliveMigrationUpstream(t *testing.T) {
	for _, version := range []independent.Version{independent.Version1, independent.Version2} {
		for _, loss := range []string{"clean", "both"} {
			t.Run(fmt.Sprintf("%x/%s", uint32(version), loss), func(t *testing.T) {
				t.Parallel()
				runIndependentMigration(t, version, loss, true)
			})
		}
	}
}

func runIndependentMigration(t *testing.T, version independent.Version, loss string, keepAlive bool) {
	t.Helper()
	clientTLS, serverTLS := tlsConfigs(t)
	peerConfig := &upstream.Config{Versions: []upstream.Version{upstream.Version(version)}}
	var trace *migrationTrace
	if mode := os.Getenv("QUIC_INTEROP_MIGRATION_TRACE"); mode != "" {
		trace = &migrationTrace{}
		peerConfig.Tracer = func(context.Context, bool, upstream.ConnectionID) qlogwriter.Trace { return trace }
		defer func() {
			if t.Failed() || mode == "all" {
				t.Log("migration trace:\n" + trace.snapshot())
			}
		}()
	}

	clientConfig := &independent.Config{Versions: []independent.Version{version}, LocalAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}}
	if keepAlive {
		peerConfig.MaxIdleTimeout = time.Second
		peerConfig.DisablePathMTUDiscovery = true
		clientConfig.MaxIdleTimeout = time.Second
		clientConfig.KeepAlivePeriod = 500 * time.Millisecond
		if version == independent.Version2 && loss == "clean" && os.Getenv("QUIC_INTEROP_KNOWN_FAILURES") == "1" {
			clientConfig.KeepAlivePeriod = 100 * time.Millisecond
		}
		if loss != "clean" {
			// Validation retries with both first flights lost need more than
			// the clean case's one-second idle budget.
			peerConfig.MaxIdleTimeout = 5 * time.Second
			clientConfig.MaxIdleTimeout = 5 * time.Second
		}
		clientConfig.DisablePathMTUDiscovery = true
	}
	listener, err := upstream.ListenAddr("127.0.0.1:0", serverTLS, peerConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	destination := listener.Addr().String()
	var relay *migrationRelay
	if loss != "clean" {
		relay = newMigrationRelayWithTrace(t, listener.Addr().(*net.UDPAddr), loss == "request" || loss == "both", loss == "response" || loss == "both", trace)
		defer relay.close()
		destination = relay.front.LocalAddr().String()
	}
	timeout := 10 * time.Second
	if keepAlive {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	client, err := independent.DialAddr(ctx, destination, clientTLS, clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseWithError(0, "")
	server, err := listener.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer server.CloseWithError(0, "")
	previous := client.LocalAddr().String()
	for round := range 3 {
		if trace != nil {
			trace.note("round=%d begin local=%s peer=%s", round, client.LocalAddr(), server.RemoteAddr())
		}
		if round > 0 {
			if err := client.Migrate(ctx, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}); err != nil {
				t.Fatalf("migration %d: %v", round, err)
			}
			if client.LocalAddr().String() == previous {
				t.Fatal("local address unchanged")
			}
			previous = client.LocalAddr().String()
			if trace != nil {
				trace.note("round=%d committed local=%s", round, previous)
			}
		}
		if keepAlive {
			// No application traffic during this interval. It exceeds both the
			// negotiated timeout and initial three-PTO floor after migration reset.
			quiet := 3500 * time.Millisecond
			if loss != "clean" {
				quiet = 5500 * time.Millisecond
			}
			timer := time.NewTimer(quiet)
			select {
			case <-client.Context().Done():
				timer.Stop()
				t.Fatal("client expired during idle round", round, context.Cause(client.Context()))
			case <-server.Context().Done():
				timer.Stop()
				t.Fatal("upstream expired during idle round", round, context.Cause(server.Context()))
			case <-timer.C:
			}
		}
		send, err := client.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		send.SetDeadline(time.Now().Add(3 * time.Second))
		payload := fmt.Sprintf("migration request %d", round)
		if _, err := send.Write([]byte(payload)); err != nil {
			t.Fatal(err)
		}
		if err := send.Close(); err != nil {
			t.Fatal(err)
		}
		receive, err := server.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		receive.SetDeadline(time.Now().Add(3 * time.Second))
		data, err := io.ReadAll(receive)
		if err != nil || string(data) != payload {
			t.Fatalf("request %q: %v", data, err)
		}
		wantPeer := previous
		if relay != nil {
			wantPeer = relay.peer(previous)
		}
		if trace != nil {
			trace.note("round=%d stream delivered peer=%s want=%s", round, server.RemoteAddr(), wantPeer)
		}
		awaitUpstreamPeerAddress(t, ctx, server, wantPeer)
		if trace != nil {
			trace.note("round=%d address confirmed peer=%s", round, server.RemoteAddr())
		}
		reply := fmt.Sprintf("migration reply %d", round)
		if _, err := receive.Write([]byte(reply)); err != nil {
			t.Fatal(err)
		}
		if err := receive.Close(); err != nil {
			t.Fatal(err)
		}
		data, err = io.ReadAll(send)
		if err != nil || string(data) != reply {
			t.Fatalf("reply %q: %v", data, err)
		}
	}
	if relay != nil {
		wantRequest, wantResponse := int32(0), int32(0)
		if loss == "request" || loss == "both" {
			wantRequest = 2
		}
		if loss == "response" || loss == "both" {
			wantResponse = 2
		}
		if relay.requestDrops.Load() != wantRequest || relay.responseDrops.Load() != wantResponse {
			t.Fatal("loss injection counts", relay.requestDrops.Load(), relay.responseDrops.Load())
		}
	}
}

// Upstream delivers STREAM data inside handleUnpackedShortHeaderPacket before
// handleShortHeaderPacket calls ChangeRemoteAddr. Application reads can wake
// between these operations, so stream completion is not an address-publication
// barrier. Still require the exact expected tuple within a bounded deadline.
func awaitUpstreamPeerAddress(t *testing.T, ctx context.Context, peer *upstream.Conn, want string) {
	t.Helper()
	wait, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if peer.RemoteAddr().String() == want {
			return
		}
		select {
		case <-ticker.C:
		case <-peer.Context().Done():
			t.Fatalf("peer closed before publishing address %s: %v", want, context.Cause(peer.Context()))
		case <-wait.Done():
			t.Fatalf("peer address %s, want %s: %v", peer.RemoteAddr(), want, wait.Err())
		}
	}
}
