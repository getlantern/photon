package interop

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

func TestPublicStreams(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", uint32(v)), func(t *testing.T) {
			if v == quic.Version2 && os.Getenv("QUIC_INTEROP_KNOWN_FAILURES") != "1" {
				t.Skip("quic-go v0.62.0 v2 key-update mismatch; set QUIC_INTEROP_KNOWN_FAILURES=1 to reproduce")
			}
			clientTLS, serverTLS := tlsConfigs(t)
			l, err := quic.ListenAddr("127.0.0.1:0", serverTLS, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			peer, err := dialUpstreamAddr(t, ctx, l.Addr().String(), clientTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}, InitialStreamReceiveWindow: 16384, MaxStreamReceiveWindow: 16384})
			if err != nil {
				t.Fatal(err)
			}
			defer peer.CloseWithError(0, "")
			c, err := l.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			incoming, err := peer.OpenStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			request := bytes.Repeat([]byte("request"), 20000)
			sent := make(chan error, 1)
			go func() {
				_, err := incoming.Write(request)
				if err == nil {
					err = incoming.Close()
				}
				sent <- err
			}()
			s, err := c.AcceptStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			s.SetReadDeadline(time.Now().Add(5 * time.Second))
			got, err := io.ReadAll(s)
			if err != nil || !bytes.Equal(got, request) {
				t.Fatal("request", len(got), err)
			}
			if err := <-sent; err != nil {
				t.Fatal(err)
			}
			response := bytes.Repeat([]byte("response"), 16000)
			// The peer is deliberately not reading yet. A deadline must return the
			// bytes committed before flow control blocked the remainder.
			s.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
			n, err := s.Write(response)
			if !errors.Is(err, os.ErrDeadlineExceeded) || n == 0 || n >= len(response) {
				t.Fatal("partial timed write", n, err)
			}
			s.SetWriteDeadline(time.Now().Add(5 * time.Second))
			received := make(chan error, 1)
			go func() {
				got, err := io.ReadAll(incoming)
				if err == nil && !bytes.Equal(got, response) {
					err = fmt.Errorf("response mismatch: %d bytes", len(got))
				}
				received <- err
			}()
			if _, err := s.Write(response[n:]); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if err := <-received; err != nil {
				t.Fatal(err)
			}
			if err := s.Release(ctx); err != nil {
				t.Fatal("release", err)
			}
			// Exercise locally opened bidirectional streams and a read deadline reset.
			local, err := c.OpenStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := local.Write([]byte("hello")); err != nil {
				t.Fatal(err)
			}
			remote, err := peer.AcceptStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			local.SetReadDeadline(time.Now().Add(-time.Second))
			if _, err := local.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatal("read deadline", err)
			}
			local.SetReadDeadline(time.Time{})
			if err := remote.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := io.ReadAll(local); err != nil {
				t.Fatal(err)
			}
			if err := local.Close(); err != nil {
				t.Fatal(err)
			}
			if b, err := io.ReadAll(remote); err != nil || string(b) != "hello" {
				t.Fatal("local stream", string(b), err)
			}
			if err := local.Release(ctx); err != nil {
				t.Fatal(err)
			}
			// Unidirectional handles expose only their legal operations.
			tx, err := c.OpenUniStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Write([]byte("uni")); err != nil {
				t.Fatal(err)
			}
			if err := tx.Close(); err != nil {
				t.Fatal(err)
			}
			rx, err := peer.AcceptUniStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if b, err := io.ReadAll(rx); err != nil || string(b) != "uni" {
				t.Fatal("uni", string(b), err)
			}
			if err := tx.Release(ctx); err != nil {
				t.Fatal(err)
			}
			peerTx, err := peer.OpenUniStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := peerTx.Write([]byte("peer uni")); err != nil {
				t.Fatal(err)
			}
			if err := peerTx.Close(); err != nil {
				t.Fatal(err)
			}
			ourRx, err := c.AcceptUniStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if b, err := io.ReadAll(ourRx); err != nil || string(b) != "peer uni" {
				t.Fatal("peer uni", string(b), err)
			}
			if err := ourRx.Release(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Small exchanges verify the public v2 stream API independently of the pinned
// upstream key-update failure. The larger test above remains a separate gate.
func TestPublicStreamSmoke(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", uint32(v)), func(t *testing.T) {
			clientTLS, serverTLS := tlsConfigs(t)
			l, err := quic.ListenAddr("127.0.0.1:0", serverTLS, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			peer, err := dialUpstreamAddr(t, ctx, l.Addr().String(), clientTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version(v)}})
			if err != nil {
				t.Fatal(err)
			}
			defer peer.CloseWithError(0, "")
			c, err := l.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			remote, err := peer.OpenStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := remote.Write([]byte("request")); err != nil {
				t.Fatal(err)
			}
			if err := remote.Close(); err != nil {
				t.Fatal(err)
			}
			s, err := c.AcceptStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			s.SetDeadline(time.Now().Add(3 * time.Second))
			if b, err := io.ReadAll(s); err != nil || string(b) != "request" {
				t.Fatal(string(b), err)
			}
			if _, err := s.Write([]byte("response")); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if b, err := io.ReadAll(remote); err != nil || string(b) != "response" {
				t.Fatal(string(b), err)
			}
			if err := s.Release(ctx); err != nil {
				t.Fatal(err)
			}
			// CancelRead wakes a blocked local reader and sends STOP_SENDING.
			outgoing, err := c.OpenStream()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := outgoing.Write([]byte("start")); err != nil {
				t.Fatal(err)
			}
			peerStream, err := peer.AcceptStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			readResult := make(chan error, 1)
			go func() { _, err := outgoing.Read(make([]byte, 1)); readResult <- err }()
			outgoing.CancelRead(7)
			select {
			case err := <-readResult:
				if err == nil {
					t.Fatal("canceled read succeeded")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			outgoing.CancelWrite(8)
			_, err = io.ReadAll(peerStream)
			var streamErr *upstream.StreamError
			if !errors.As(err, &streamErr) || streamErr.ErrorCode != 8 {
				t.Fatal("reset code", err)
			}
			if err := outgoing.Release(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPublicStreamAutomaticCleanup(t *testing.T) {
	clientTLS, serverTLS := tlsConfigs(t)
	l, err := quic.ListenAddr("127.0.0.1:0", serverTLS, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// v1 isolates public lifecycle testing from the documented upstream v2
	// key-update defect. Engine lifecycle tests run both versions for 300 streams.
	peer, err := dialUpstreamAddr(t, ctx, l.Addr().String(), clientTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version1}})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.CloseWithError(0, "")
	c, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var first *quic.ReceiveStream
	for i := 0; i < 40; i++ {
		tx, err := peer.OpenUniStreamSync(ctx)
		if err != nil {
			t.Fatal(i, err)
		}
		if _, err := tx.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		if err := tx.Close(); err != nil {
			t.Fatal(err)
		}
		rx, err := c.AcceptUniStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		rx.SetReadDeadline(time.Now().Add(time.Second))
		if b, err := io.ReadAll(rx); err != nil || string(b) != "x" {
			t.Fatal(i, string(b), err)
		}
		if i == 0 {
			first = rx
		}
		// No Release call: more than the initial 16 incoming streams must work.
	}
	first.SetReadDeadline(time.Time{})
	if n, err := first.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatal("EOF after automatic cleanup", n, err)
	}
	for i := 0; i < 160; i++ {
		tx, err := c.OpenUniStreamSync(ctx)
		if err != nil {
			t.Fatal(i, err)
		}
		if _, err := tx.Write([]byte("y")); err != nil {
			t.Fatal(err)
		}
		if err := tx.Close(); err != nil {
			t.Fatal(err)
		}
		rx, err := peer.AcceptUniStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if b, err := io.ReadAll(rx); err != nil || string(b) != "y" {
			t.Fatal(i, string(b), err)
		}
		// Exercise sequential automatic cleanup without explicit Release.
	}
}

func TestPublicOpenWaitsForPeerCredit(t *testing.T) {
	clientTLS, serverTLS := tlsConfigs(t)
	l, err := upstream.ListenAddr("127.0.0.1:0", serverTLS, &upstream.Config{MaxIncomingStreams: 100, MaxIncomingUniStreams: 100})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := quic.DialAddr(ctx, l.Addr().String(), clientTLS, &quic.Config{Versions: []quic.Version{quic.Version1}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseWithError(0, "")
	peer, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer peer.CloseWithError(0, "")
	var release *quic.Stream
	for i := 0; i < 100; i++ {
		s, err := c.OpenStream()
		if err != nil {
			t.Fatal(i, err)
		}
		_, err = c.OpenUniStream()
		if err != nil {
			t.Fatal(i, err)
		}
		if i == 0 {
			release = s
		}
	}
	short, stop := context.WithTimeout(ctx, 20*time.Millisecond)
	if _, err := c.OpenStreamSync(short); !errors.Is(err, context.DeadlineExceeded) {
		stop()
		t.Fatal("peer-credit wait did not honor cancellation", err)
	}
	stop()
	type result struct {
		s   *quic.Stream
		err error
	}
	opened := make(chan result, 1)
	go func() { s, err := c.OpenStreamSync(ctx); opened <- result{s, err} }()
	if err := release.Close(); err != nil {
		t.Fatal(err)
	}
	rx, err := peer.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(rx); err != nil {
		t.Fatal(err)
	}
	if err := rx.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(release); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-opened:
		if r.err != nil || r.s.StreamID() != 400 {
			t.Fatal("open after peer credit replenishment", r.s, r.err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
