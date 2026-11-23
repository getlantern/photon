package http3conn

import (
	"errors"
	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/wire"
	"sync"
	"testing"
)

func TestSettingsDirection(t *testing.T) {
	enabled := []h3.Setting{{ID: h3.EnableConnectProtocol, Value: 1}}
	for _, server := range []bool{false, true} {
		for _, local := range []bool{false, true} {
			for _, peer := range []bool{false, true} {
				var sent, received []h3.Setting
				if local {
					sent = enabled
				}
				if peer {
					received = enabled
				}
				s, err := NewSettingsState(server, sent)
				if err != nil {
					t.Fatal(err)
				}
				if s.CanSendExtendedConnect() || s.CanReceiveExtendedConnect() != (server && local) {
					t.Fatal("pre-settings permissions")
				}
				p, ok := s.Peer()
				if ok || p.MaxFieldSectionSize != ^uint64(0) || p.QPACKCapacity != 0 {
					t.Fatal("defaults")
				}
				if err := s.ApplyPeer(received); err != nil {
					t.Fatal(err)
				}
				if s.CanSendExtendedConnect() != (!server && peer) || s.CanReceiveExtendedConnect() != (server && local) {
					t.Fatal("wrong direction", server, local, peer)
				}
			}
		}
	}
}
func TestSettingsFailureAtomic(t *testing.T) {
	s, _ := NewSettingsState(false, nil)
	err := s.ApplyPeer([]h3.Setting{{ID: h3.QPACKMaxTableCapacity, Value: 100}, {ID: h3.EnableConnectProtocol, Value: 2}})
	var protocol *h3.Error
	if !errors.As(err, &protocol) || protocol.Code != h3.SettingsError {
		t.Fatal(err)
	}
	p, ok := s.Peer()
	if ok || p.QPACKCapacity != 0 || s.ApplyPeer(nil) != err {
		t.Fatal("partial mutation or lost error")
	}
	s, _ = NewSettingsState(false, nil)
	s.ApplyPeer([]h3.Setting{{ID: h3.EnableConnectProtocol, Value: 1}})
	err = s.ApplyPeer(nil)
	if !errors.As(err, &protocol) || protocol.Code != h3.FrameUnexpected || s.CanSendExtendedConnect() {
		t.Fatal("duplicate settings", err)
	}
}
func TestSettingsConcurrentPublication(t *testing.T) {
	s, _ := NewSettingsState(false, nil)
	settings := []h3.Setting{{ID: h3.QPACKMaxTableCapacity, Value: 4096}, {ID: h3.QPACKBlockedStreams, Value: 8}, {ID: h3.MaxFieldSectionSize, Value: 65536}, {ID: h3.EnableConnectProtocol, Value: 1}, {ID: h3.H3Datagram, Value: 1}}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Go(func() {
			for j := 0; j < 100; j++ {
				p, ready := s.Peer()
				if ready && (p.QPACKCapacity != 4096 || p.QPACKBlockedStreams != 8 || p.MaxFieldSectionSize != 65536 || !p.ExtendedConnect || !p.HTTPDatagrams) {
					t.Error("torn publication")
				}
				s.CanSendExtendedConnect()
				s.CanReceiveExtendedConnect()
				s.CanSendHTTPDatagrams()
				s.Local()
			}
		})
	}
	if err := s.ApplyPeer(settings); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	settings[0].Value = 1
	p, _ := s.Peer()
	if p.QPACKCapacity != 4096 {
		t.Fatal("borrowed mutable settings")
	}
}
func TestExtendedConnectSettingCodec(t *testing.T) {
	for _, v := range []uint64{0, 1, 2, 255, wire.MaxVarint} {
		payload, _ := wire.AppendVarint([]byte{8}, v)
		_, err := h3.ParseSettings(payload, 1)
		prefix := []byte{42}
		encoded, encErr := h3.AppendSettings(prefix, []h3.Setting{{ID: 8, Value: uint64(v)}})
		if v < 2 {
			if err != nil || encErr != nil {
				t.Fatal(v, err, encErr)
			}
		} else {
			var e *h3.Error
			if !errors.As(err, &e) || e.Code != h3.SettingsError || !errors.As(encErr, &e) || len(encoded) != 1 || encoded[0] != 42 {
				t.Fatal("invalid setting accepted or output mutated")
			}
		}
	}
}
