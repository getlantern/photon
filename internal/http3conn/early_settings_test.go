package http3conn

import (
	"errors"
	h3 "github.com/getlantern/photon/internal/http3wire"
	"sync"
	"testing"
)

func TestEarlySettingsCompatibility(t *testing.T) {
	old := []h3.Setting{{ID: h3.QPACKMaxTableCapacity, Value: 4096}, {ID: h3.QPACKBlockedStreams, Value: 8}, {ID: h3.MaxFieldSectionSize, Value: 65536}, {ID: h3.EnableConnectProtocol, Value: 1}, {ID: h3.H3Datagram, Value: 1}}
	for _, tc := range []struct {
		name  string
		index int
		value uint64
		omit  bool
		code  uint64
	}{
		{"same", -1, 0, false, 0}, {"capacity reduced", 0, 2048, false, QPACKDecoderStreamError}, {"capacity increased", 0, 8192, false, QPACKDecoderStreamError}, {"capacity omitted", 0, 0, true, QPACKDecoderStreamError},
		{"blocked reduced", 1, 7, false, h3.SettingsError}, {"blocked increased", 1, 9, false, 0}, {"blocked omitted", 1, 0, true, h3.SettingsError},
		{"fields reduced", 2, 65535, false, h3.SettingsError}, {"fields increased", 2, 65537, false, 0}, {"fields omitted", 2, 0, true, h3.SettingsError},
		{"connect disabled", 3, 0, false, h3.SettingsError}, {"connect omitted", 3, 0, true, h3.SettingsError}, {"datagrams disabled", 4, 0, false, h3.SettingsError}, {"datagrams omitted", 4, 0, true, h3.SettingsError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current := append([]h3.Setting(nil), old...)
			if tc.index >= 0 {
				if tc.omit {
					current = append(current[:tc.index], current[tc.index+1:]...)
				} else {
					current[tc.index].Value = tc.value
				}
			}
			state, err := NewEarlySettingsState(nil, old)
			if err != nil {
				t.Fatal(err)
			}
			remembered, ok := state.EarlyPeer()
			if !ok || remembered.QPACKCapacity != 4096 {
				t.Fatal("no remembered limits")
			}
			if _, ok := state.Peer(); ok {
				t.Fatal("remembered data impersonated SETTINGS")
			}
			err = state.ApplyPeer(current)
			if tc.code == 0 {
				if err != nil {
					t.Fatal(err)
				}
				if _, ok := state.Peer(); !ok {
					t.Fatal("live SETTINGS missing")
				}
			} else {
				var protocol *h3.Error
				if !errors.As(err, &protocol) || protocol.Code != tc.code {
					t.Fatal("wrong error", err)
				}
				if _, ok := state.Peer(); ok {
					t.Fatal("invalid SETTINGS published")
				}
				if state.ApplyPeer(old) != err {
					t.Fatal("terminal error lost")
				}
			}
			if _, ok := state.EarlyPeer(); ok {
				t.Fatal("early permissions survived transition")
			}
		})
	}
}

func TestEarlySettingsOwnershipAndDefaults(t *testing.T) {
	remembered := []h3.Setting{{ID: h3.MaxFieldSectionSize, Value: 100}, {ID: 0x21, Value: 77}, {ID: 0xabcdef, Value: 99}}
	s, err := NewEarlySettingsState(nil, remembered)
	if err != nil {
		t.Fatal(err)
	}
	remembered[0].Value = 1
	if err := s.ApplyPeer([]h3.Setting{{ID: h3.MaxFieldSectionSize, Value: 100}, {ID: h3.QPACKMaxTableCapacity, Value: 4096}, {ID: h3.QPACKBlockedStreams, Value: 8}}); err != nil {
		t.Fatal("zero capacity can increase; unknown settings may disappear", err)
	}
	defaults, err := RememberSettings(nil)
	if err != nil || defaults.Snapshot().MaxFieldSectionSize != ^uint64(0) {
		t.Fatal("bad defaults", err)
	}
	if err := defaults.ValidateAccepted(nil); err != nil {
		t.Fatal(err)
	}
	if err := defaults.ValidateAccepted([]h3.Setting{{ID: h3.MaxFieldSectionSize, Value: 100}}); err == nil {
		t.Fatal("default unlimited limit reduced")
	}
	if _, err := NewEarlySettingsState(nil, []h3.Setting{{ID: h3.H3Datagram, Value: 2}}); err == nil {
		t.Fatal("invalid remembered setting")
	}
	if _, err := RememberSettings([]h3.Setting{{ID: 99}, {ID: 99}}); err == nil {
		t.Fatal("duplicate remembered setting")
	}
	// Recovery has no remembered constraints: new values can shrink or disappear.
	fresh, _ := NewSettingsState(false, nil)
	if err := fresh.ApplyPeer(nil); err != nil {
		t.Fatal(err)
	}
}

func TestEarlySettingsConcurrentTransition(t *testing.T) {
	s, _ := NewEarlySettingsState(nil, []h3.Setting{{ID: h3.MaxFieldSectionSize, Value: 100}})
	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 100; j++ {
				s.EarlyPeer()
				s.Peer()
				s.CanSendHTTPDatagrams()
				s.CanSendExtendedConnect()
			}
		}()
	}
	if err := s.ApplyPeer([]h3.Setting{{ID: h3.MaxFieldSectionSize, Value: 200}}); err != nil {
		t.Fatal(err)
	}
	workers.Wait()
}
