package interop

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	quic "github.com/getlantern/photon"
	upstream "github.com/quic-go/quic-go"
)

func TestPublicDialUpstream(t *testing.T) {
	for _, v := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", uint32(v)), func(t *testing.T) {
			clientTLS, serverTLS := tlsConfigs(t)
			listener, err := upstream.ListenAddr("127.0.0.1:0", serverTLS, &upstream.Config{EnableDatagrams: true, Versions: []upstream.Version{upstream.Version(v)}})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			dialCtx, dialCancel := context.WithCancel(ctx)
			c, err := quic.DialAddr(dialCtx, listener.Addr().String(), clientTLS, &quic.Config{Versions: []quic.Version{v}, EnableDatagrams: true})
			dialCancel()
			if err != nil {
				t.Fatal(err)
			}
			defer c.CloseWithError(0, "")
			peer, err := listener.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer peer.CloseWithError(0, "")
			if c.ConnectionState().Version != v || !c.ConnectionState().TLS.HandshakeComplete || !c.ConnectionState().SupportsDatagrams {
				t.Fatal("connection state")
			}
			if err := c.SendDatagram([]byte("datagram")); err != nil {
				t.Fatal(err)
			}
			if b, err := peer.ReceiveDatagram(ctx); err != nil || string(b) != "datagram" {
				t.Fatal(string(b), err)
			}
			if err := peer.SendDatagram([]byte("reply")); err != nil {
				t.Fatal(err)
			}
			if b, err := c.ReceiveDatagram(ctx); err != nil || string(b) != "reply" {
				t.Fatal(string(b), err)
			}
			s, err := c.OpenStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			s.SetDeadline(time.Now().Add(3 * time.Second))
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
			if b, err := io.ReadAll(ps); err != nil || string(b) != "request" {
				t.Fatal(string(b), err)
			}
			if _, err := ps.Write([]byte("response")); err != nil {
				t.Fatal(err)
			}
			if err := ps.Close(); err != nil {
				t.Fatal(err)
			}
			if b, err := io.ReadAll(s); err != nil || string(b) != "response" {
				t.Fatal(string(b), err)
			}
			stoppedSend, err := c.OpenUniStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := stoppedSend.Write([]byte("stop")); err != nil {
				t.Fatal(err)
			}
			stoppedReceive, err := peer.AcceptUniStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			stoppedReceive.CancelRead(27)
			select {
			case <-stoppedSend.Context().Done():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var stopped *quic.StreamError
			if !errors.As(context.Cause(stoppedSend.Context()), &stopped) || !stopped.Remote || stopped.ErrorCode != 27 || stopped.StreamID != stoppedSend.StreamID() {
				t.Fatal("stream context cause", context.Cause(stoppedSend.Context()))
			}
			resetSender, err := peer.OpenUniStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			resetSender.CancelWrite(61)
			resetReceiver, err := c.AcceptUniStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			resetReceiver.SetReadDeadline(time.Now().Add(time.Second))
			var reset *quic.StreamError
			if _, err := resetReceiver.Read(make([]byte, 1)); !errors.As(err, &reset) || !reset.Remote || reset.ErrorCode != 61 || reset.StreamID != resetReceiver.StreamID() {
				t.Fatal("reset read cause", err)
			}
			if err := resetReceiver.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := resetReceiver.Read(make([]byte, 1)); !errors.As(err, &reset) || reset.ErrorCode != 61 || !reset.Remote {
				t.Fatal("reset after cleanup", err)
			}
			if err := peer.CloseWithError(19, "complete"); err != nil {
				t.Fatal(err)
			}
			select {
			case <-c.Context().Done():
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}

func TestPublicDialUpstreamVersionRejection(t *testing.T) {
	clientTLS, serverTLS := tlsConfigs(t)
	l, err := upstream.ListenAddr("127.0.0.1:0", serverTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version1}})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c, err := quic.DialAddr(ctx, l.Addr().String(), clientTLS, &quic.Config{Versions: []quic.Version{quic.Version2}})
	if c != nil {
		c.CloseWithError(0, "")
		t.Fatal("unexpected connection")
	}
	var rejected *quic.VersionNegotiationError
	if !errors.As(err, &rejected) {
		t.Fatal("expected VN error", err)
	}
	if !errors.Is(err, net.ErrClosed) || len(rejected.Ours) != 1 || rejected.Ours[0] != quic.Version2 {
		t.Fatalf("local version details: %+v", rejected)
	}
	found := false
	for _, v := range rejected.Theirs {
		found = found || v == quic.Version1
	}
	if !found {
		t.Fatalf("missing independently offered version: %+v", rejected)
	}
}

func TestPublicDialUpstreamFallback(t *testing.T) {
	for _, only := range []quic.Version{quic.Version1, quic.Version2} {
		t.Run(fmt.Sprintf("%x", uint32(only)), func(t *testing.T) {
			if only == quic.Version2 && os.Getenv("QUIC_INTEROP_VN_FAILURES") != "1" {
				t.Skip("quic-go v0.62.0 omits version_information after v2 fallback; set QUIC_INTEROP_VN_FAILURES=1 to reproduce")
			}
			clientTLS, serverTLS := tlsConfigs(t)
			l, err := upstream.ListenAddr("127.0.0.1:0", serverTLS, &upstream.Config{Versions: []upstream.Version{upstream.Version(only)}})
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			first := quic.Version1
			if only == quic.Version1 {
				first = quic.Version2
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			c, err := quic.DialAddr(ctx, l.Addr().String(), clientTLS, &quic.Config{Versions: []quic.Version{first, only}})
			if err != nil {
				t.Fatal(err)
			}
			defer c.CloseWithError(0, "")
			if c.ConnectionState().Version != only {
				t.Fatal("wrong negotiated version")
			}
			peer, err := l.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer peer.CloseWithError(0, "")
			s, err := c.OpenUniStreamSync(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Write([]byte("fallback")); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			rx, err := peer.AcceptUniStream(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if b, err := io.ReadAll(rx); err != nil || string(b) != "fallback" {
				t.Fatal(string(b), err)
			}
		})
	}
}
