package quic

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/transport"
)

func TestPublicStreamContexts(t *testing.T) {
	for _, v := range []Version{Version1, Version2} {
		clientTLS, serverTLS := dialTLS(t)
		l, err := ListenAddr("127.0.0.1:0", serverTLS, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c, err := DialAddr(ctx, l.Addr().String(), clientTLS, &Config{Versions: []Version{v}})
		if err != nil {
			t.Fatal(err)
		}
		defer c.server.Close()
		peer, err := l.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		s, err := c.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if s.Context().Err() != nil {
			t.Fatal("new stream context canceled")
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if context.Cause(s.Context()) != ErrStreamClosed || s.Context().Err() != context.Canceled {
			t.Fatal("normal close cause", context.Cause(s.Context()), s.Context().Err())
		}
		ps, err := peer.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ps.Write([]byte("read side remains open")); err != nil {
			t.Fatal(err)
		}
		if err := ps.Close(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-s.ReceiveContext().Done():
		case <-ctx.Done():
			t.Fatal("receive FIN not observed before read", ctx.Err())
		}
		if context.Cause(s.ReceiveContext()) != io.EOF {
			t.Fatal(context.Cause(s.ReceiveContext()))
		}
		s.SetReadDeadline(time.Now().Add(time.Second))
		if b, err := io.ReadAll(s); err != nil || string(b) != "read side remains open" {
			t.Fatal(string(b), err)
		}
		local, err := c.OpenUniStream()
		if err != nil {
			t.Fatal(err)
		}
		local.CancelWrite(9)
		local.CancelWrite(10)
		var stopped *StreamError
		if !errors.As(context.Cause(local.Context()), &stopped) || stopped.Remote || stopped.ErrorCode != 9 || stopped.StreamID != local.StreamID() {
			t.Fatal("local cancellation", context.Cause(local.Context()))
		}
		outgoing, err := c.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := outgoing.Write([]byte("start")); err != nil {
			t.Fatal(err)
		}
		receiver, err := peer.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		receiver.CancelRead(42)
		select {
		case <-outgoing.Context().Done():
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if !errors.As(context.Cause(outgoing.Context()), &stopped) || !stopped.Remote || stopped.ErrorCode != 42 || stopped.StreamID != outgoing.StreamID() {
			t.Fatal("remote cancellation", context.Cause(outgoing.Context()))
		}
		if _, err := outgoing.Write([]byte("late")); !errors.As(err, &stopped) || stopped.ErrorCode != 42 {
			t.Fatal("write lost cancellation", err)
		}
		alive, err := c.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		c.server.Close()
		select {
		case <-alive.Context().Done():
		case <-ctx.Done():
			t.Fatal("connection did not cancel stream", ctx.Err())
		}
		select {
		case <-alive.ReceiveContext().Done():
		case <-ctx.Done():
			t.Fatal("connection did not cancel receive context")
		}
		if context.Cause(alive.ReceiveContext()) != context.Cause(c.Context()) {
			t.Fatal("lost receive connection cause")
		}
		if context.Cause(alive.Context()) != context.Cause(c.Context()) {
			t.Fatal("lost connection cause")
		}
	}
}

func TestPublicReceiveErrorsAfterCleanup(t *testing.T) {
	for _, v := range []Version{Version1, Version2} {
		clientTLS, serverTLS := dialTLS(t)
		l, err := ListenAddr("127.0.0.1:0", serverTLS, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c, err := DialAddr(ctx, l.Addr().String(), clientTLS, &Config{Versions: []Version{v}})
		if err != nil {
			t.Fatal(err)
		}
		defer c.server.Close()
		peer, err := l.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, remote := range []bool{false, true} {
			tx, err := c.OpenUniStream()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Write([]byte("x")); err != nil {
				t.Fatal(err)
			}
			rx, err := peer.AcceptUniStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			rx.SetReadDeadline(time.Now().Add(time.Second))
			// Consume existing data before testing cancellation of a pending read.
			if _, err := io.ReadFull(rx, make([]byte, 1)); err != nil {
				t.Fatal(err)
			}
			pending := make(chan error, 1)
			go func() { _, err := rx.Read(make([]byte, 1)); pending <- err }()
			if remote {
				tx.CancelWrite(32)
			} else {
				rx.CancelRead(32)
			}
			select {
			case err := <-pending:
				var stopped *StreamError
				if !errors.As(err, &stopped) || stopped.ErrorCode != 32 || stopped.StreamID != rx.StreamID() || stopped.Remote != remote {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case <-rx.ReceiveContext().Done():
			case <-ctx.Done():
				t.Fatal("reset did not cancel receive context")
			}
			var receiveCause *StreamError
			if !errors.As(context.Cause(rx.ReceiveContext()), &receiveCause) || receiveCause.ErrorCode != 32 || receiveCause.Remote != remote || receiveCause.StreamID != rx.StreamID() {
				t.Fatal(context.Cause(rx.ReceiveContext()))
			}
			if err := rx.Release(ctx); err != nil {
				t.Fatal(err)
			}
			rx.CancelRead(99)
			var stopped *StreamError
			if _, err := rx.Read(make([]byte, 1)); !errors.As(err, &stopped) || stopped.ErrorCode != 32 || stopped.Remote != remote || stopped.StreamID != rx.StreamID() {
				t.Fatal("error lost after cleanup", err)
			}
		}
	}
}

// holdingRelay forwards UDP between one client and server, and can hold
// server-to-client datagrams until released. It counts client datagrams.
type holdingRelay struct {
	socket  *net.UDPConn
	mu      sync.Mutex
	holding bool
	held    [][]byte
	client  *net.UDPAddr
	fromCli atomic.Int64
}

func newHoldingRelay(t *testing.T, server *net.UDPAddr) *holdingRelay {
	t.Helper()
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	r := &holdingRelay{socket: socket}
	done := make(chan struct{})
	go func() {
		defer close(done)
		buffer := make([]byte, 65535)
		for {
			n, from, err := socket.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			packet := append([]byte(nil), buffer[:n]...)
			if from.AddrPort() != server.AddrPort() {
				r.mu.Lock()
				r.client = from
				r.mu.Unlock()
				r.fromCli.Add(1)
				_, _ = socket.WriteToUDP(packet, server)
				continue
			}
			r.mu.Lock()
			client := r.client
			if r.holding || client == nil {
				r.held = append(r.held, packet)
				r.mu.Unlock()
				continue
			}
			r.mu.Unlock()
			_, _ = socket.WriteToUDP(packet, client)
		}
	}()
	t.Cleanup(func() { socket.Close(); <-done })
	return r
}

func (r *holdingRelay) hold() { r.mu.Lock(); r.holding = true; r.mu.Unlock() }

func (r *holdingRelay) release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.holding = false
	for _, packet := range r.held {
		_, _ = r.socket.WriteToUDP(packet, r.client)
	}
	r.held = nil
}

// STOP_SENDING that arrives after a stream's FIN is queued does not make
// Release report a failure: sending had already ended with the FIN, and
// Release waits only for completion, an acknowledged reset included. The send
// Context keeps the FIN as its cause. HTTP/3 relies on this, see
// http3/README.md. A relay holds the server's datagrams, so the client cannot
// have the FIN when it cancels and must send STOP_SENDING. The hold ends only
// once a client datagram sent after CancelRead, which carries STOP_SENDING,
// has reached the server, before the client could acknowledge the FIN.
func TestReleaseAfterStopSendingFollowingFIN(t *testing.T) {
	clientTLS, serverTLS := dialTLS(t)
	l, err := ListenAddr("127.0.0.1:0", serverTLS, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	relay := newHoldingRelay(t, l.Addr().(*net.UDPAddr))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, err := DialAddr(ctx, relay.socket.LocalAddr().String(), clientTLS, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.server.Close()
	peer, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 5; round++ {
		s, err := c.OpenStream()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Write([]byte("request")); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		ps, err := peer.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadAll(ps); err != nil {
			t.Fatal(err)
		}
		relay.hold()
		if _, err := ps.Write([]byte("response")); err != nil {
			t.Fatal(err)
		}
		if err := ps.Close(); err != nil {
			t.Fatal(err)
		}
		sent := relay.fromCli.Load()
		s.CancelRead(0x10c) // the FIN is held, so this sends STOP_SENDING
		for relay.fromCli.Load() == sent {
			select {
			case <-ctx.Done():
				t.Fatal("round", round, "STOP_SENDING never sent", ctx.Err())
			case <-time.After(time.Millisecond):
			}
		}
		relay.release()
		if err := ps.Release(ctx); err != nil {
			t.Fatal("round", round, "Release after STOP_SENDING following FIN:", err)
		}
		if cause := context.Cause(ps.Context()); cause != ErrStreamClosed {
			t.Fatal("round", round, "send context cause:", cause)
		}
		// The server processed the client's STOP_SENDING: the reset it
		// answered with is the stream's recorded send outcome.
		var terminal error
		if err := peer.server.Do(ctx, func(*transport.ServerEndpoint) error { terminal = ps.SendStream.sendTerminal; return nil }); err != nil {
			t.Fatal(err)
		}
		var reset *StreamError
		if !errors.As(terminal, &reset) || reset.ErrorCode != 0x10c {
			t.Fatal("round", round, "STOP_SENDING not processed after FIN; recorded send outcome:", terminal)
		}
	}
}
