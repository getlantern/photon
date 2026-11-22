package transport

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"hash"
)

// resetTokenIndex is source-owner state. Map keys are secret-key HMAC values,
// never raw tokens; a candidate must still pass live tuple and constant-time
// token checks. The shared MAC and scratch space require serialized access.
type resetTokenIndex struct {
	mac         hash.Hash
	digest      [sha256.Size]byte
	token       [16]byte
	scratch     map[[sha256.Size]byte]struct{}
	tokens      map[[sha256.Size]byte]map[*Conn]struct{}
	connections map[*Conn]map[[sha256.Size]byte]struct{}
}

func newResetTokenIndex() (*resetTokenIndex, error) {
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		return nil, err
	}
	index := &resetTokenIndex{
		mac:         hmac.New(sha256.New, key[:]),
		scratch:     make(map[[sha256.Size]byte]struct{}),
		tokens:      make(map[[sha256.Size]byte]map[*Conn]struct{}),
		connections: make(map[*Conn]map[[sha256.Size]byte]struct{}),
	}
	index.mac.Reset() // Precompute reusable keyed states outside packet lookup.
	return index, nil
}

func (index *resetTokenIndex) key(token []byte) [sha256.Size]byte {
	index.mac.Reset()
	_, _ = index.mac.Write(token)
	index.mac.Sum(index.digest[:0])
	return index.digest
}

func (index *resetTokenIndex) candidates(data []byte) map[*Conn]struct{} {
	if len(data) < 21 {
		return nil
	}
	return index.tokens[index.key(data[len(data)-16:])]
}

func (index *resetTokenIndex) remove(c *Conn) {
	for key := range index.connections[c] {
		delete(index.tokens[key], c)
		if len(index.tokens[key]) == 0 {
			delete(index.tokens, key)
		}
	}
	delete(index.connections, c)
}

func (index *resetTokenIndex) sync(c *Conn) {
	if c.closed || c.Draining() {
		index.remove(c)
		return
	}
	clear(index.scratch)
	add := func(ids *peerIDs) {
		if ids == nil {
			return
		}
		for _, entry := range ids.active {
			if !entry.used || !entry.hasToken {
				continue
			}
			index.token = entry.token
			index.scratch[index.key(index.token[:])] = struct{}{}
		}
	}
	if c.multipath == nil {
		add(c.peerIDs)
	} else {
		for id, state := range c.multipath.paths {
			if c.multipath.abandoned[id] == nil {
				add(state.peer)
			}
		}
	}
	old := index.connections[c]
	for key := range old {
		if _, ok := index.scratch[key]; !ok {
			delete(index.tokens[key], c)
			if len(index.tokens[key]) == 0 {
				delete(index.tokens, key)
			}
			delete(old, key)
		}
	}
	for key := range index.scratch {
		if _, ok := old[key]; ok {
			continue
		}
		if old == nil {
			old = make(map[[sha256.Size]byte]struct{})
			index.connections[c] = old
		}
		if index.tokens[key] == nil {
			index.tokens[key] = make(map[*Conn]struct{})
		}
		old[key] = struct{}{}
		index.tokens[key][c] = struct{}{}
	}
	if len(old) == 0 {
		delete(index.connections, c)
	}
}

// Enable lazily only for a runtime that dispatches foreign CID traffic. Bootstrap
// precedes the first lookup; subsequent successful Router.Sync calls update one
// connection, including first-use tokens before packet exposure to its socket.
func (r *Router) enableResetIndex() {
	if r.resetIndex != nil {
		return
	}
	index, err := newResetTokenIndex()
	if err != nil {
		return
	} // Selection retains its complete scan fallback.
	for c := range r.entries {
		index.sync(c)
	}
	r.resetIndex = index
}
