package transport

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/netip"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestResetKeyVector(t *testing.T) {
	var key ResetKey
	for i := 0; i < 20; i++ {
		key[i] = 0x0b
	}
	got := key.token([]byte("Hi There"))
	// RFC 4231 test case 1, HMAC-SHA256 truncated to 128 bits. Extending a
	// short HMAC key with zero bytes to 32 bytes preserves the padded key.
	if hex.EncodeToString(got[:]) != "b0344c61d8db38535ca8afceaf0bf12b" {
		t.Fatal("HMAC vector")
	}
	if key.token([]byte("other CID")) == got {
		t.Fatal("CID not bound")
	}
	key[0]++
	if key.token([]byte("Hi There")) == got {
		t.Fatal("key not bound")
	}
}

func TestResetKeyHandshakeAndPathIssuance(t *testing.T) {
	for _, version := range []wire.Version{wire.Version1, wire.Version2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			client, old := testPair(t, version)
			defer client.Close()
			config := old.config
			old.Close()
			key := ResetKey{7}
			expected := key
			config.ResetKey = &key
			config.EnableMigration = true
			config.preferredSocketsReady = true
			config.PreferredAddresses = []netip.AddrPort{netip.MustParseAddrPort("192.0.2.1:443")}
			clientConfig := client.config
			clientConfig.ResetKey = &key
			client.Close()
			client, err := NewConn(clientConfig)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			if err := client.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			server, err := NewConn(config)
			if err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			key[0]++ // The connection owns a snapshot, not the caller's storage.
			if err := server.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			drivePair(t, client, server, false, false)
			if server.peerParams.StatelessResetToken != nil {
				t.Fatal("client advertised server-only reset parameter")
			}
			for seq, id := range client.localIDs.active {
				if seq != 0 && (!id.hasToken || id.token != expected.token(id.cid)) {
					t.Fatal("client CID token lost key")
				}
			}
			preferred := client.peerParams.PreferredAddress
			if preferred == nil || preferred.StatelessResetToken != expected.token(preferred.ConnectionID) {
				t.Fatal("preferred-address token lost key")
			}
			token := client.peerParams.StatelessResetToken
			if token == nil || *token != expected.token(server.config.LocalCID) {
				t.Fatal("initial token not advertised")
			}
			if len(server.localIDs.active) < 2 {
				t.Fatal("no additional IDs issued")
			}
			for _, id := range server.localIDs.active {
				if !id.hasToken || id.token != expected.token(id.cid) {
					t.Fatal("nonreproducible local token")
				}
			}
			paths, err := adoptPathZero(server, 3, 3, 4)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := paths.issue(1); err != nil {
				t.Fatal(err)
			}
			for _, id := range paths.paths[1].local.active {
				if !id.hasToken || id.token != expected.token(id.cid) {
					t.Fatal("multipath lost reset key")
				}
			}
		})
	}
}
