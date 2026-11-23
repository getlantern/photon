package quic

import (
	"context"
	"errors"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestVersionRejectionPreferenceSnapshot(t *testing.T) {
	for _, first := range []Version{Version1, Version2} {
		ct, _ := dialTLS(t)
		socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		defer socket.Close()
		if err := socket.SetDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		offered := []Version{0xfaceb00c, 0x1a2a3a4a}
		done := make(chan error, 1)
		go func() {
			b := make([]byte, 65535)
			n, addr, err := socket.ReadFromUDP(b)
			if err != nil {
				done <- err
				return
			}
			h, err := wire.ParseHeader(b[:n], 8)
			if err != nil {
				done <- err
				return
			}
			response, err := wire.AppendVersionNegotiation(nil, 0x80, h.SCID, h.DCID, offered)
			if err == nil {
				_, err = socket.WriteToUDP(response, addr)
			}
			done <- err
		}()
		versions := []Version{Version1, Version2}
		if first == Version2 {
			slices.Reverse(versions)
		}
		want := slices.Clone(versions)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		c, err := DialAddr(ctx, socket.LocalAddr().String(), ct, &Config{Versions: versions})
		cancel()
		if c != nil {
			c.CloseWithError(0, "")
			t.Fatal("unexpected connection")
		}
		if peerErr := <-done; peerErr != nil {
			t.Fatal(peerErr)
		}
		var rejection *VersionNegotiationError
		if !errors.As(err, &rejection) || !errors.Is(err, net.ErrClosed) {
			t.Fatal("rejection identity", err)
		}
		if rejection.ChosenVersion != first || !slices.Equal(rejection.Ours, want) || !slices.Equal(rejection.Theirs, offered) || !slices.Equal(rejection.PeerVersions, offered) {
			t.Fatalf("rejection details: %+v", rejection)
		}
		versions[0] = 0
		if !slices.Equal(rejection.Ours, want) {
			t.Fatal("error aliases caller preferences")
		}
		rejection.Theirs[0] = 0
		if !slices.Equal(rejection.PeerVersions, offered) {
			t.Fatal("new and legacy offer snapshots alias")
		}
	}
}
