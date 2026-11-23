package transport

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/handshake"
	"github.com/getlantern/photon/internal/wire"
)

func TestReceiveFailureAutomaticallyCloses(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, tlsFailure := range []bool{false, true} {
			client, server := testPair(t, v)
			now := time.Unix(200, 0)
			var frame wire.Frame = wire.MaxDataFrame{Maximum: 1} // Illegal in Initial.
			if tlsFailure {
				frame = wire.CryptoFrame{Data: []byte{0xff, 0, 0, 0}}
			}
			raw := makePeerPacket(t, client, initialSpace, 10, frame)
			err := server.HandleDatagram(raw, now)
			if err == nil || !server.Closing() {
				t.Fatal("failure did not initiate close", err)
			}
			var code, trigger uint64
			if tlsFailure {
				var alert tls.AlertError
				if !errors.As(err, &alert) {
					t.Fatal("expected TLS alert", err)
				}
				code = 0x100 + uint64(alert)
			} else {
				code = 0xa
				trigger = 0x10
			}
			b, sendErr := server.NextDatagram(now)
			if sendErr != nil || len(b) != 1200 {
				t.Fatal("close output", len(b), sendErr)
			}
			h, parseErr := wire.ParseHeader(b, len(client.config.LocalCID))
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			_, plain, _, openErr := client.readKeys(initialSpace).Open(b, h.PNOffset, 0)
			if openErr != nil {
				t.Fatal(openErr)
			}
			f, _, parseErr := wire.ConsumeFrame(plain)
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			close := f.(wire.ConnectionCloseFrame)
			if close.Application || close.ErrorCode != code || close.TriggerFrame != trigger {
				t.Fatal(close)
			}
			if tlsFailure && len(close.Reason) != 0 {
				t.Fatal("TLS diagnostics exposed")
			}
			if server.err != err {
				t.Fatal("original error replaced")
			}
			if err := client.HandleDatagram(b, now); !errors.Is(err, ErrPeerClosed) {
				t.Fatal("peer did not receive close", err)
			}
		}
	}
}

func TestReceiveFailureWithoutUsableKeys(t *testing.T) {
	c, _ := testPair(t, wire.Version1)
	limit, _ := c.initialWrite.UsageLimits()
	c.sealed[initialSpace] = limit
	original := c.fail(&TransportError{Code: 7, Reason: "malformed frame"})
	c.closeReceiveFailure(original, time.Unix(200, 0))
	if c.Closing() || c.err != original || c.sealed[initialSpace] != limit {
		t.Fatal("exhausted keys used or original error lost")
	}
}

func TestCryptoReassemblyFailureCloses(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		for _, conflict := range []bool{false, true} {
			client, server := testPair(t, v)
			now := time.Unix(200, 0)
			frame := wire.CryptoFrame{Offset: 65536, Data: []byte{1}}
			code := uint64(0xd)
			if conflict {
				frame.Offset = 10
				first := makePeerPacket(t, client, initialSpace, 0, frame)
				if err := server.HandleDatagram(first, now); err != nil {
					t.Fatal(err)
				}
				frame.Data = []byte{2}
				code = 0xa
			}
			raw := makePeerPacket(t, client, initialSpace, 1, frame)
			if err := server.HandleDatagram(raw, now); err == nil || !server.Closing() {
				t.Fatal("missing CRYPTO close", err)
			}
			assertInitialCloseCode(t, server, client, now, code, 6)
		}
	}
}

func assertInitialCloseCode(t *testing.T, sender, receiver *Conn, now time.Time, code, trigger uint64) {
	t.Helper()
	raw, err := sender.NextDatagram(now)
	if err != nil || len(raw) == 0 {
		t.Fatal("missing close", err)
	}
	h, err := wire.ParseHeader(raw, len(receiver.config.LocalCID))
	if err != nil {
		t.Fatal(err)
	}
	_, plain, _, err := receiver.readKeys(initialSpace).Open(raw, h.PNOffset, 0)
	if err != nil {
		t.Fatal(err)
	}
	f, _, err := wire.ConsumeFrame(plain)
	if err != nil {
		t.Fatal(err)
	}
	close := f.(wire.ConnectionCloseFrame)
	if close.Application || close.ErrorCode != code || close.TriggerFrame != trigger {
		t.Fatal(close)
	}
}

func TestTransportParameterBindingFailureCloses(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := testPair(t, v)
		now := time.Unix(200, 0)
		// TLS advertises the original client CID. The packet header and the
		// server's routing context instead agree on another CID of the same length.
		other := []byte("changed0")
		client.config.LocalCID = other
		server.initialPeerCID = other
		server.peerCID = other
		var result error
		for _, b := range drain(t, client, now) {
			result = server.HandleDatagram(b, now)
			if result != nil {
				break
			}
		}
		if !errors.Is(result, wire.ErrTransportParameter) || !server.Closing() {
			t.Fatal("missing parameter close", result)
		}
		assertInitialCloseCode(t, server, client, now, 8, 0)
	}
}

func TestAcceptedEarlyLimitFailureCloses(t *testing.T) {
	for _, v := range []wire.Version{wire.Version1, wire.Version2} {
		client, server := testPair(t, v)
		now := time.Unix(100, 0)
		original := client.fail(wire.ErrEarlyLimits)
		client.closeReceiveFailure(original, now)
		if !client.Closing() || client.err != original {
			t.Fatal("early limit failure did not close")
		}
		assertInitialCloseCode(t, client, server, now, 0xa, 0)
	}
}

func TestReceiveFailureCodeConsistency(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		for _, test := range []struct {
			cause       error
			code, frame uint64
		}{
			{&TransportError{Code: 7, FrameType: 8, Reason: "bad frame"}, 7, 8},
			{wire.ErrEarlyLimits, 0xa, 0},
			{wire.ErrTransportParameter, 8, 0},
			{handshake.ErrCryptoBuffer, 0xd, 6},
			{handshake.ErrCryptoConflict, 0xa, 6},
			{fmt.Errorf("private TLS diagnostic: %w", tls.AlertError(42)), 0x12a, 0},
		} {
			sender, receiver := testPair(t, version)
			now := time.Unix(200, 0)
			got := sender.closeReceiveFailure(test.cause, now)
			var failure *ConnectionTransportError
			if !errors.As(got, &failure) || failure.Remote || uint64(failure.ErrorCode) != test.code || failure.FrameType != test.frame || !errors.Is(got, test.cause) || !errors.Is(got, net.ErrClosed) || sender.err != got {
				t.Fatal("terminal failure", got)
			}
			assertInitialCloseCode(t, sender, receiver, now, test.code, test.frame)
		}
	}
}

func TestReceiveFailureDoesNotRespondToRemoteOrUnclassifiedErrors(t *testing.T) {
	for _, cause := range []error{
		&ConnectionTransportError{Remote: true, ErrorCode: 7},
		&ApplicationError{Remote: true, ErrorCode: 7},
		&PeerCloseError{Code: 7},
		ErrStatelessReset,
		errors.New("socket failure"),
	} {
		conn, _ := testPair(t, wire.Version1)
		got := conn.closeReceiveFailure(cause, time.Unix(200, 0))
		if got != cause || conn.Closing() || conn.err != nil {
			t.Fatal("unexpected protocol close", got)
		}
	}
}

func TestReceiveFailureKeepsFirstWireCode(t *testing.T) {
	sender, receiver := testPair(t, wire.Version1)
	now := time.Unix(200, 0)
	first := sender.fail(&TransportError{Code: 7, FrameType: 8, Reason: "first failure"})
	got := sender.closeReceiveFailure(&TransportError{Code: 0xa, FrameType: 6, Reason: "later failure"}, now)
	if got != first {
		t.Fatal("first failure replaced", got)
	}
	assertInitialCloseCode(t, sender, receiver, now, 7, 8)
}
