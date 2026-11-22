package transport

import (
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/token"
	"github.com/getlantern/photon/internal/wire"
)

func TestAdmissionContextOwnership(t *testing.T) {
	client, unused := testPair(t, wire.Version1)
	defer client.Close()
	unused.Close()
	now := time.Now()
	packets := drain(t, client, now)
	retry, _ := token.NewRetryTokens([32]byte{9}, nil, time.Minute)
	peer := netip.MustParseAddrPort("192.0.2.1:443")
	var input context.Context
	calls := 0
	template := Config{Version: wire.Version1, TLS: unused.config.TLS, Parameters: wire.DefaultTransportParameters(), VerifySourceAddress: func(netip.AddrPort) bool { return false }, ConnContext: func(ctx context.Context, _ netip.AddrPort, _ bool) (context.Context, error) {
		calls++
		input = ctx
		return context.Background(), nil
	}}
	invalid := append([]byte(nil), packets[0]...)
	invalid[len(invalid)-1] ^= 1
	c, _, err := AdmitInitial(context.Background(), template, retry, invalid, peer, now)
	if c != nil || err != nil || calls != 0 {
		t.Fatal("unauthenticated hook", c, err, calls)
	}
	parent, cancel := context.WithCancel(context.Background())
	c, _, err = AdmitInitial(parent, template, retry, packets[0], peer, now)
	if c == nil || err != nil || calls != 1 {
		t.Fatal(c, err, calls)
	}
	defer c.Close()
	cancel()
	if input.Err() != nil || c.lifetimeContext.Err() != nil {
		t.Fatal("socket loop controls connection context")
	}
	sentinel := errors.New("terminal connection failure")
	c.fail(sentinel)
	for _, ctx := range []context.Context{input, c.lifetimeContext} {
		if !errors.Is(context.Cause(ctx), sentinel) {
			t.Fatal("lost independent lifetime cancellation", context.Cause(ctx))
		}
	}
}
