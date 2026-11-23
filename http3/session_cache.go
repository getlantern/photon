package http3

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"sync"

	"github.com/getlantern/photon/internal/http3conn"
	h3 "github.com/getlantern/photon/internal/http3wire"
	"github.com/getlantern/photon/internal/wire"
)

const httpTicketPrefix = "independent-quic/http3/settings/1\x00"

// scope binds the canonical HTTPS authority and the local HTTP policy. Transport
// ticket metadata independently binds the QUIC version. This is client-local
// metadata, never a replacement for the server's authenticated ticket contents.
func encodeHTTPSettings(scope string, p http3conn.SettingsSnapshot) []byte {
	return encodeHTTPSettingsRecord(httpTicketPrefix, scope, p)
}
func encodeHTTPSettingsRecord(prefix, scope string, p http3conn.SettingsSnapshot) []byte {
	if len(scope) > 4096 {
		return nil
	}
	b := append([]byte(prefix), byte(len(scope)>>8), byte(len(scope)))
	b = append(b, scope...)
	b = binary.BigEndian.AppendUint64(b, p.QPACKCapacity)
	b = binary.BigEndian.AppendUint64(b, p.QPACKBlockedStreams)
	b = binary.BigEndian.AppendUint64(b, p.MaxFieldSectionSize)
	b = append(b, 0, 0)
	if p.ExtendedConnect {
		b[len(b)-2] = 1
	}
	if p.HTTPDatagrams {
		b[len(b)-1] = 1
	}
	return b
}
func decodeHTTPSettings(extra [][]byte, scope string) ([]h3.Setting, bool) {
	return decodeHTTPSettingsRecord(httpTicketPrefix, extra, scope)
}
func decodeHTTPSettingsRecord(prefix string, extra [][]byte, scope string) ([]h3.Setting, bool) {
	var record []byte
	for _, b := range extra {
		if bytes.HasPrefix(b, []byte(prefix)) {
			if record != nil {
				return nil, false
			}
			record = b[len(prefix):]
		}
	}
	if len(record) < 2 {
		return nil, false
	}
	n := int(binary.BigEndian.Uint16(record))
	record = record[2:]
	if n > 4096 || len(record) != n+26 || string(record[:n]) != scope {
		return nil, false
	}
	b := record[n:]
	capacity, blocked, fields := binary.BigEndian.Uint64(b), binary.BigEndian.Uint64(b[8:]), binary.BigEndian.Uint64(b[16:])
	if capacity > wire.MaxVarint || blocked > wire.MaxVarint || (fields > wire.MaxVarint && fields != ^uint64(0)) || b[24] > 1 || b[25] > 1 {
		return nil, false
	}
	result := []h3.Setting{{ID: h3.QPACKMaxTableCapacity, Value: capacity}, {ID: h3.QPACKBlockedStreams, Value: blocked}, {ID: h3.EnableConnectProtocol, Value: uint64(b[24])}, {ID: h3.H3Datagram, Value: uint64(b[25])}}
	if fields != ^uint64(0) {
		result = append(result, h3.Setting{ID: h3.MaxFieldSectionSize, Value: fields})
	}
	return result, true
}

// httpSessionCache is per connection. A ticket received before peer SETTINGS is
// retained privately (at most one) until publishSettings pairs the two. Underlying
// caches remain shareable; no cached SessionState or Extra slice is mutated.
// Endpoint integration must publish only successfully validated peer settings.
type httpSessionCache struct {
	base     tls.ClientSessionCache
	scope    string
	mu       sync.Mutex
	peer     *http3conn.SettingsSnapshot
	pending  *httpPendingTicket
	restored []h3.Setting
}
type httpPendingTicket struct {
	key    string
	ticket []byte
	state  *tls.SessionState
}

func cloneHTTPSession(session *tls.ClientSessionState) ([]byte, *tls.SessionState, error) {
	ticket, state, err := session.ResumptionState()
	if err != nil {
		return nil, nil, err
	}
	data, err := state.Bytes()
	if err != nil {
		return nil, nil, err
	}
	state, err = tls.ParseSessionState(data)
	return append([]byte(nil), ticket...), state, err
}
func (c *httpSessionCache) Get(key string) (*tls.ClientSessionState, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.restored = nil
	session, ok := c.base.Get(key)
	if !ok || session == nil {
		return nil, false
	}
	ticket, state, err := cloneHTTPSession(session)
	if err != nil {
		return nil, false
	}
	settings, valid := decodeHTTPSettings(state.Extra, c.scope)
	state.EarlyData = state.EarlyData && valid
	if state.EarlyData {
		c.restored = settings
	}
	result, err := tls.NewResumptionState(ticket, state)
	return result, err == nil
}
func (c *httpSessionCache) Put(key string, session *tls.ClientSessionState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if session == nil {
		c.pending = nil
		c.base.Put(key, nil)
		return
	}
	ticket, state, err := cloneHTTPSession(session)
	if err != nil {
		return
	}
	pending := &httpPendingTicket{key: key, ticket: ticket, state: state}
	if c.peer == nil {
		c.pending = pending
		return
	}
	c.store(pending)
}
func (c *httpSessionCache) store(p *httpPendingTicket) {
	// Remove inherited HTTP records so a refreshed ticket never carries stale or
	// ambiguous settings. Keep all other protocols' and QUIC's opaque metadata.
	extra := make([][]byte, 0, len(p.state.Extra)+1)
	for _, b := range p.state.Extra {
		if !bytes.HasPrefix(b, []byte(httpTicketPrefix)) {
			extra = append(extra, b)
		}
	}
	record := encodeHTTPSettings(c.scope, *c.peer)
	if record == nil {
		p.state.EarlyData = false
	} else {
		extra = append(extra, record)
	}
	p.state.Extra = extra
	session, err := tls.NewResumptionState(p.ticket, p.state)
	if err == nil {
		c.base.Put(p.key, session)
	}
}
func (c *httpSessionCache) publishSettings(peer http3conn.SettingsSnapshot) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.peer != nil {
		return
	}
	c.peer = &peer
	if c.pending != nil {
		c.store(c.pending)
		c.pending = nil
	}
}
func (c *httpSessionCache) rememberedSettings() ([]h3.Setting, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]h3.Setting(nil), c.restored...), c.restored != nil
}
