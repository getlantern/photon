package http3

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestCachedOnlyAdmission(t *testing.T) {
	transport := &Transport{MaxConnections: 1}
	defer transport.Close()
	body := &poolTestBody{Reader: strings.NewReader("not sent")}
	req, _ := http.NewRequest("POST", "https://127.0.0.1:1", body)
	if response, err := transport.RoundTripOpt(req, RoundTripOpt{OnlyCachedConn: true}); response != nil || !errors.Is(err, ErrNoCachedConn) {
		t.Fatal(response, err)
	}
	if body.closed.Load() != 1 || len(transport.all) != 0 || len(transport.pool) != 0 {
		t.Fatal("miss touched pool or leaked body")
	}
	socket, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	entry, err := transport.acquire(context.Background(), socket.LocalAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	joined, err := transport.acquireWithOptions(context.Background(), socket.LocalAddr().String(), RoundTripOpt{OnlyCachedConn: true})
	if err != nil || joined != entry || entry.refs != 2 {
		t.Fatal("did not join existing dial", err)
	}
	if _, err := transport.acquireWithOptions(context.Background(), "other:443", RoundTripOpt{OnlyCachedConn: true}); !errors.Is(err, ErrNoCachedConn) {
		t.Fatal("miss became capacity error", err)
	}
	if entry.retired || len(transport.all) != 1 {
		t.Fatal("miss retired existing connection")
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	waitingBody := &poolTestBody{Reader: strings.NewReader("waiting")}
	waiting, _ := http.NewRequestWithContext(waitCtx, "POST", "https://"+socket.LocalAddr().String(), waitingBody)
	if response, err := transport.RoundTripOpt(waiting, RoundTripOpt{OnlyCachedConn: true}); response != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(response, err)
	}
	transport.mu.Lock()
	refs, retired := entry.refs, entry.retired
	transport.mu.Unlock()
	if refs != 2 || retired || entry.ctx.Err() != nil || waitingBody.closed.Load() != 1 {
		t.Fatal("canceled cached waiter damaged shared dial", refs, retired)
	}
	transport.release(joined)
	transport.release(entry)
}
