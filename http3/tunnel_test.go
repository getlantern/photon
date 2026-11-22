package http3

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestOpenTunnelRejectsInvalidRequestBeforeConnection(t *testing.T) {
	for _, mode := range []string{"nil", "method", "body", "length", "trailer", "transfer", "length-header", "trailer-header", "transfer-header"} {
		t.Run(mode, func(t *testing.T) {
			r, _ := http.NewRequest("CONNECT", "https://proxy.example", nil)
			switch mode {
			case "nil":
				r = nil
			case "method":
				r.Method = "GET"
			case "body":
				r.Body = io.NopCloser(strings.NewReader("data"))
			case "length":
				r.ContentLength = 3
			case "trailer":
				r.Trailer = http.Header{"X-Trailer": nil}
			case "transfer":
				r.TransferEncoding = []string{"chunked"}
			case "length-header":
				r.Header["content-length"] = []string{"0"}
			case "trailer-header":
				r.Header["trailer"] = []string{"x-checksum"}
			case "transfer-header":
				r.Header["transfer-encoding"] = []string{"chunked"}
			}
			if tunnel, response, err := new(ClientConn).OpenTunnel(r); err == nil || tunnel != nil || response != nil {
				t.Fatal(tunnel, response, err)
			}
		})
	}
}

func TestTunnelCloseUnblocksWrite(t *testing.T) {
	input, output := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tunnel := &Tunnel{reader: &tunnelCloseReader{input: input}, writer: output, ctx: ctx}
	done := make(chan error, 1)
	go func() { _, err := tunnel.Write([]byte("blocked")); done <- err }()
	if err := tunnel.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("write survived close")
		}
	case <-time.After(time.Second):
		t.Fatal("close did not unblock write")
	}
	if err := tunnel.Close(); err != nil {
		t.Fatal(err)
	}
}

type tunnelCloseReader struct{ input *io.PipeReader }

func (r *tunnelCloseReader) Read([]byte) (int, error) { return 0, io.EOF }
func (r *tunnelCloseReader) Close() error             { return r.input.Close() }
