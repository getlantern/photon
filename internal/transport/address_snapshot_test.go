package transport

import (
	"net/netip"
	"sync"
	"testing"

	"github.com/getlantern/photon/internal/wire"
)

func TestRemoteAddressSnapshotConcurrentAndAfterClose(t *testing.T) {
	c, _ := testPair(t, wire.Version1, streamParameters())
	first := netip.MustParseAddrPort("192.0.2.1:443")
	second := netip.MustParseAddrPort("192.0.2.1:444")
	if c.CurrentRemoteAddr().IsValid() {
		t.Fatal("snapshot populated before address binding")
	}
	c.publishRemoteAddress(first)
	var readers sync.WaitGroup
	for range 4 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for range 2000 {
				address := c.CurrentRemoteAddr()
				if address != first && address != second {
					t.Errorf("invalid snapshot: %v", address)
					return
				}
			}
		}()
	}
	for range 1000 {
		c.publishRemoteAddress(second)
		c.publishRemoteAddress(first)
	}
	readers.Wait()
	c.publishRemoteAddress(second)
	c.Close()
	if c.CurrentRemoteAddr() != second {
		t.Fatal("close lost last committed address")
	}
}

func TestPathAddressSnapshotConsistentAndRetained(t *testing.T) {
	c, _ := testPair(t, wire.Version1, streamParameters())
	first := pathTuple{netip.MustParseAddrPort("192.0.2.1:443"), netip.MustParseAddrPort("192.0.2.2:444")}
	second := pathTuple{netip.MustParseAddrPort("192.0.2.3:445"), netip.MustParseAddrPort("192.0.2.4:446")}
	if _, _, ok := c.CurrentPathAddresses(1); ok {
		t.Fatal("unbound path reported")
	}
	c.publishPathAddress(1, first)
	c.publishPathAddress(2, second)
	var readers sync.WaitGroup
	for range 4 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for range 2000 {
				local, remote, ok := c.CurrentPathAddresses(1)
				got := pathTuple{local, remote}
				if !ok || (got != first && got != second) {
					t.Error("inconsistent address pair")
					return
				}
			}
		}()
	}
	for range 1000 {
		c.publishPathAddress(1, second)
		c.publishPathAddress(1, first)
	}
	readers.Wait()
	c.Close()
	for id, want := range map[uint64]pathTuple{1: first, 2: second} {
		local, remote, ok := c.CurrentPathAddresses(id)
		if !ok || (pathTuple{local, remote}) != want {
			t.Fatal("snapshot lost binding", id)
		}
	}
}
