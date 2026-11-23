package transport

import (
	"errors"
	"testing"
	"time"

	"github.com/getlantern/photon/internal/wire"
)

func TestConfidentialityLimitUpdatesBeforeReuse(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	keys := client.writeKeys(applicationSpace)
	limit, _ := keys.UsageLimits()
	client.sealed[applicationSpace] = limit - 1
	client.enqueue(applicationSpace, wire.PingFrame{}, 1)
	client.enqueue(applicationSpace, wire.PingFrame{}, 1)
	now := time.Unix(200, 0)
	if b, err := client.NextDatagram(now); err != nil || len(b) == 0 {
		t.Fatal(err)
	}
	if client.sealed[applicationSpace] != limit || client.writeKeys(applicationSpace) != keys {
		t.Fatal("last allowed packet not counted")
	}
	if b, err := client.NextDatagram(now); err != nil || len(b) == 0 {
		t.Fatal(err)
	}
	if client.sealed[applicationSpace] != 1 || client.writeKeys(applicationSpace) == keys {
		t.Fatal("exhausted key reused")
	}
}
func TestConfidentialityLimitStopsWhenUpdateUnavailable(t *testing.T) {
	client, _ := testPair(t, wire.Version1)
	limit, _ := client.writeKeys(initialSpace).UsageLimits()
	client.sealed[initialSpace] = limit
	b, err := client.NextDatagram(time.Unix(200, 0))
	var transport *TransportError
	if len(b) != 0 || !errors.As(err, &transport) || transport.Code != 0xf {
		t.Fatal("exhausted Initial key used", err)
	}
}
func TestIntegrityLimitCountsPacketsAcrossKeyTrials(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	_, limit := client.readKeys(applicationSpace).UsageLimits()
	client.integrityLimit = limit
	client.failedAuth = limit - 2
	now := time.Unix(200, 0)
	raw := makePeerPacket(t, server, applicationSpace, 1000, wire.PingFrame{})
	raw[len(raw)-1] ^= 1
	if err := client.HandleDatagram(raw, now); err != nil {
		t.Fatal(err)
	}
	if client.failedAuth != limit-1 {
		t.Fatal("one packet counted once per key trial", client.failedAuth)
	}
	raw = makePeerPacket(t, server, applicationSpace, 1001, wire.PingFrame{})
	raw[len(raw)-1] ^= 1
	err := client.HandleDatagram(raw, now)
	var transport *TransportError
	if !errors.As(err, &transport) || transport.Code != 0xf {
		t.Fatal("integrity exhaustion did not stop connection", err)
	}
	if b, err := client.NextDatagram(now); len(b) != 0 || err == nil {
		t.Fatal("output after integrity exhaustion")
	}
}
func TestValidNewKeyPacketDoesNotCountAsForgery(t *testing.T) {
	client, server := testPair(t, wire.Version1)
	drivePair(t, client, server, false, false)
	client.failedAuth = 10
	if err := server.InitiateKeyUpdate(); err != nil {
		t.Fatal(err)
	}
	for _, b := range drain(t, server, time.Unix(200, 0)) {
		if err := client.HandleDatagram(b, time.Unix(200, 0)); err != nil {
			t.Fatal(err)
		}
	}
	if client.failedAuth != 10 {
		t.Fatal("valid next-key packet counted as forgery")
	}
	if err := client.InitiateKeyUpdate(); err == nil {
		t.Fatal("update without current-phase ACK unexpectedly permitted")
	}
	if client.failedAuth != 10 {
		t.Fatal("key update reset integrity counter")
	}
}
