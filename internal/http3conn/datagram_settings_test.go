package http3conn

import (
	"context"
	"fmt"
	"testing"
	"time"

	h3 "github.com/getlantern/photon/internal/http3wire"
)

func TestHTTPDatagramSettingsNegotiation(t *testing.T) {
	settings := func(v int) []h3.Setting {
		if v < 0 {
			return nil
		}
		return []h3.Setting{{ID: h3.H3Datagram, Value: uint64(v)}}
	}
	for _, server := range []bool{false, true} {
		for _, local := range []int{-1, 0, 1} {
			for _, remote := range []int{-1, 0, 1} {
				t.Run(fmt.Sprintf("server=%v/local=%d/peer=%d", server, local, remote), func(t *testing.T) {
					s, err := NewSettingsState(server, settings(local))
					if err != nil {
						t.Fatal(err)
					}
					if s.CanSendHTTPDatagrams() {
						t.Fatal("send before peer settings")
					}
					if err := s.ApplyPeer(settings(remote)); err != nil {
						t.Fatal(err)
					}
					peer, ok := s.Peer()
					if !ok || peer.HTTPDatagrams != (remote == 1) || s.Local().HTTPDatagrams != (local == 1) {
						t.Fatal("snapshot")
					}
					if s.CanSendHTTPDatagrams() != (local == 1 && remote == 1) {
						t.Fatal("incorrect negotiation gate")
					}
					if err := s.ApplyPeer(settings(remote)); err == nil || s.CanSendHTTPDatagrams() {
						t.Fatal("duplicate settings retained permission")
					}
				})
			}
		}
	}
	s, _ := NewSettingsState(false, settings(1))
	if err := s.ApplyPeer(settings(2)); err == nil || s.CanSendHTTPDatagrams() {
		t.Fatal("invalid setting granted permission")
	}
	if _, ready := s.Peer(); ready {
		t.Fatal("failed settings published")
	}
}

func TestSessionPublishesHTTPDatagramNegotiation(t *testing.T) {
	transport := &dispatchTransport{streams: make(chan ReceiveStream, 1)}
	enabled := []h3.Setting{{ID: h3.H3Datagram, Value: 1}}
	local := &LocalStreams{Control: &fakeStream{interrupt: make(chan struct{})}, Encoder: &fakeStream{interrupt: make(chan struct{})}, Decoder: &fakeStream{interrupt: make(chan struct{})}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	session, err := StartSession(ctx, transport, local, SessionConfig{PeerIsServer: true, LocalSettings: enabled, Decoder: decoderLimits(), Encoder: encoderLimits(), MaxActiveStreams: 4, MaxSettingsBytes: 1024, MaxSettingsEntries: 8, MaxEncoderInstruction: 1024, Control: func(context.Context, h3.ControlEvent) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Stop(context.Canceled)
	if session.Settings().CanSendHTTPDatagrams() {
		t.Fatal("enabled before control stream")
	}
	data, err := h3.AppendSettings([]byte{0}, enabled)
	if err != nil {
		t.Fatal(err)
	}
	transport.streams <- heldSessionStream(3, data)
	if _, err := session.WaitEncoder(ctx); err != nil {
		t.Fatal(err)
	}
	if !session.Settings().CanSendHTTPDatagrams() {
		t.Fatal("parsed settings not published")
	}
}
