package http3

import (
	"bytes"
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/getlantern/photon/internal/http3conn"
	h3 "github.com/getlantern/photon/internal/http3wire"
)

type capturedHTTPSession struct{ saved chan *tls.ClientSessionState }

func (c *capturedHTTPSession) Get(string) (*tls.ClientSessionState, bool) { return nil, false }
func (c *capturedHTTPSession) Put(_ string, s *tls.ClientSessionState) {
	if s != nil {
		select {
		case c.saved <- s:
		default:
		}
	}
}
func cacheTestTicket(t *testing.T) *tls.ClientSessionState {
	t.Helper()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) }))
	defer server.Close()
	cache := &capturedHTTPSession{saved: make(chan *tls.ClientSessionState, 2)}
	cfg := server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	cfg.ClientSessionCache = cache
	conn, err := tls.Dial("tcp", strings.TrimPrefix(server.URL, "https://"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, conn); err != nil {
		t.Fatal(err)
	}
	var saved *tls.ClientSessionState
	select {
	case saved = <-cache.saved:
	default:
		t.Fatal("no TLS ticket")
	}
	ticket, state, err := cloneHTTPSession(saved)
	if err != nil {
		t.Fatal(err)
	}
	state.EarlyData = true
	state.Extra = append(state.Extra, []byte("opaque transport metadata"))
	saved, err = tls.NewResumptionState(ticket, state)
	if err != nil {
		t.Fatal(err)
	}
	return saved
}
func TestHTTPSettingsTicketCache(t *testing.T) {
	source := cacheTestTicket(t)
	_, state, _ := source.ResumptionState()
	before, _ := state.Bytes()
	base := tls.NewLRUClientSessionCache(4)
	c := &httpSessionCache{base: base, scope: "example.com:443/policy-a"}
	c.Put("tls-key", source)
	if ticket, ok := base.Get("tls-key"); ok && ticket != nil {
		t.Fatal("ticket published before settings")
	}
	c.publishSettings(http3conn.SettingsSnapshot{QPACKCapacity: 4096, QPACKBlockedStreams: 8, MaxFieldSectionSize: 65536, HTTPDatagrams: true})
	c.publishSettings(http3conn.SettingsSnapshot{}) // First validated SETTINGS owns the ticket.
	got, ok := c.Get("tls-key")
	if !ok {
		t.Fatal("ticket missing")
	}
	_, private, _ := got.ResumptionState()
	if !private.EarlyData {
		t.Fatal("early capability lost")
	}
	remembered, ok := c.rememberedSettings()
	if !ok {
		t.Fatal("remembered settings missing")
	}
	remembered[0].Value = 1
	fresh, _ := c.rememberedSettings()
	if fresh[0].Value != 4096 {
		t.Fatal("restored settings aliased")
	}
	after, _ := state.Bytes()
	if !bytes.Equal(before, after) {
		t.Fatal("shared TLS state mutated")
	}
	for _, scope := range []string{"other.example:443/policy-a", "example.com:444/policy-a", "example.com:443/policy-b"} {
		other := &httpSessionCache{base: base, scope: scope}
		ticket, ok := other.Get("tls-key")
		if !ok {
			t.Fatal("ordinary resumption lost")
		}
		_, s, _ := ticket.ResumptionState()
		if s.EarlyData {
			t.Fatal("early data crossed scope")
		}
	}
	c.Put("tls-key", nil)
	if ticket, ok := base.Get("tls-key"); ok && ticket != nil {
		t.Fatal("cache deletion lost")
	}
	c2 := &httpSessionCache{base: base, scope: c.scope}
	c2.Put("tls-key", source)
	c2.Put("tls-key", nil)
	c2.publishSettings(http3conn.SettingsSnapshot{})
	if ticket, ok := base.Get("tls-key"); ok && ticket != nil {
		t.Fatal("deleted pending ticket restored")
	}
}
func TestHTTPSettingsRecordValidation(t *testing.T) {
	p := http3conn.SettingsSnapshot{MaxFieldSectionSize: ^uint64(0)}
	b := encodeHTTPSettings("scope", p)
	if _, ok := decodeHTTPSettings([][]byte{b}, "scope"); !ok {
		t.Fatal("default record invalid")
	}
	for i := 0; i < len(b); i++ {
		if _, ok := decodeHTTPSettings([][]byte{b[:i]}, "scope"); ok {
			t.Fatal("truncated record accepted", i)
		}
	}
	if _, ok := decodeHTTPSettings([][]byte{b, b}, "scope"); ok {
		t.Fatal("ambiguous record accepted")
	}
	bad := bytes.Clone(b)
	bad[len(bad)-1] = 2
	if _, ok := decodeHTTPSettings([][]byte{bad}, "scope"); ok {
		t.Fatal("invalid boolean accepted")
	}
	r, _ := http3conn.RememberSettings([]h3.Setting{{ID: h3.QPACKMaxTableCapacity, Value: 99}})
	if _, ok := decodeHTTPSettings([][]byte{encodeHTTPSettings("scope", r.Snapshot())}, "scope"); !ok {
		t.Fatal("known values lost")
	}
}
func TestHTTPSettingsCacheConcurrentPublication(t *testing.T) {
	source := cacheTestTicket(t)
	base := tls.NewLRUClientSessionCache(4)
	c := &httpSessionCache{base: base, scope: "scope"}
	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 20; j++ {
				c.Put("key", source)
				c.Get("key")
				c.rememberedSettings()
			}
		}()
	}
	c.publishSettings(http3conn.SettingsSnapshot{MaxFieldSectionSize: 100})
	workers.Wait()
	got, ok := c.Get("key")
	if !ok {
		t.Fatal("missing ticket")
	}
	_, state, _ := got.ResumptionState()
	if !state.EarlyData {
		t.Fatal("missing early state")
	}
}
