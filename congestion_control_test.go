package quic

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

// Two endpoints configured for BBR complete a transfer: the option reaches
// both the dialing and the listening side.
func TestCongestionControlBBRTransfer(t *testing.T) {
	ct, st := dialTLS(t)
	config := &Config{CongestionControl: CongestionControlBBR}
	l, err := ListenAddr("127.0.0.1:0", st, config)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := DialAddr(ctx, l.Addr().String(), ct, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseWithError(0, "")
	server, err := l.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer server.CloseWithError(0, "")
	payload := bytes.Repeat([]byte("bbr"), 200_000)
	stream, err := client.OpenUniStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		stream.Write(payload)
		stream.Close()
	}()
	in, err := server.AcceptUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	in.SetReadDeadline(time.Now().Add(10 * time.Second))
	got, err := io.ReadAll(in)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatal("transfer", len(got), err)
	}
}
