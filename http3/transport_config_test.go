package http3

import (
	"net/http"
	"strings"
	"testing"

	quic "github.com/getlantern/photon"
)

func TestTransportProtocolOptions(t *testing.T) {
	for _, tc := range []struct {
		name      string
		tr        *Transport
		headers   int
		datagrams bool
	}{
		{name: "default", tr: &Transport{}, headers: 10 << 20},
		{name: "negative-default", tr: &Transport{MaxResponseHeaderBytes: -1}, headers: 10 << 20},
		{name: "legacy-limit", tr: &Transport{ClientConfig: &ClientConfig{MaxHeaderBytes: 1024}}, headers: 1024},
		{name: "override", tr: &Transport{MaxResponseHeaderBytes: 2048, ClientConfig: &ClientConfig{MaxHeaderBytes: 1024}}, headers: 2048},
		{name: "automatic-datagrams", tr: &Transport{EnableDatagrams: true}, headers: 10 << 20, datagrams: true},
		{name: "explicit-datagrams", tr: &Transport{EnableDatagrams: true, QUICConfig: &quic.Config{EnableDatagrams: true}}, headers: 10 << 20, datagrams: true},
		{name: "legacy-datagrams", tr: &Transport{ClientConfig: &ClientConfig{EnableDatagrams: true}, QUICConfig: &quic.Config{}}, headers: 10 << 20, datagrams: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var before ClientConfig
			if tc.tr.ClientConfig != nil {
				before = *tc.tr.ClientConfig
			}
			cfg, err := tc.tr.clientOptions()
			if err != nil || cfg.responseHeaderLimit() != tc.headers || cfg.EnableDatagrams != tc.datagrams {
				t.Fatalf("headers=%d datagrams=%t err=%v", cfg.responseHeaderLimit(), cfg.EnableDatagrams, err)
			}
			wantRequestLimit := 65536
			if before.MaxHeaderBytes != 0 {
				wantRequestLimit = before.MaxHeaderBytes
			}
			if cfg.MaxHeaderBytes != wantRequestLimit {
				t.Fatal("response limit changed request header budget")
			}
			if tc.tr.ClientConfig != nil && (tc.tr.ClientConfig.MaxHeaderBytes != before.MaxHeaderBytes || tc.tr.ClientConfig.EnableDatagrams != before.EnableDatagrams) {
				t.Fatal("mutated caller options")
			}
		})
	}
	tr := &Transport{EnableDatagrams: true, QUICConfig: &quic.Config{}}
	body := &poolTestBody{Reader: strings.NewReader("hello")}
	request, _ := http.NewRequest("POST", "https://example.com/", body)
	if response, err := tr.RoundTrip(request); response != nil || err == nil {
		t.Fatal("disabled QUIC datagrams accepted", response, err)
	}
	if body.closed.Load() != 1 || len(tr.all) != 0 || tr.QUICConfig.EnableDatagrams {
		t.Fatal("invalid configuration consumed resources or changed caller configuration")
	}
}
